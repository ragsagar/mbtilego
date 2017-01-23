package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)
const VERSION = "0.2.0"
const DEG_TO_RAD = math.Pi / 180
const RAD_TO_DEG = 180 / math.Pi
const MAX_LATITUDE = 85.0511287798
const DEFAULT_TILE_SIZE = 256
const MAX_ZOOM_LEVEL = 17
const PNG_IMAGE_FORMAT = "image/png"
const JPG_IMAGE_FORMAT = "image/jpg"
const JPG_EXTENSION = "jpg"
const PNG_EXTENSION = "png"
const MBTILE_VERSION = "1.2"

var MAPTYPES = []string{"http://mt2.google.com/vt/lyrs=y&x={x}&y={y}&z={z}", "http://c.tile.openstreetmap.org/{z}/{x}/{y}.png", "http://a.tiles.mapbox.com/v4/rsagar.n724o8le/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoicnNhZ2FyIiwiYSI6IjM5OWVlZTVlYzJiYjhmMzAyMGMwMDBiYzA4NjEzMWM3In0.gc0JW6Ddp0RD_yBaaPE1vg"}
var MAP_IMAGE_TYPES = []string{JPG_IMAGE_FORMAT, PNG_IMAGE_FORMAT, PNG_IMAGE_FORMAT}

type Tile struct {
	z, x, y int
	Content []byte
}

func (tile *Tile) flipped_y() int {
	two_power_zoom := math.Pow(2.0, float64(tile.z))
	return int(two_power_zoom) - 1 - tile.y
}

func mbTileWorker(db *sql.DB, tilePipe chan Tile, outputPipe chan Tile) {
	for {
		tile := <-tilePipe
		err := addToMBTile(tile, db)
		if err != nil {
			log.Fatal(err)
		}
		outputPipe <- tile
	}
}

func addToMBTile(tile Tile, db *sql.DB) error {
	_, err := db.Exec("insert into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);", tile.z, tile.x, tile.flipped_y(), tile.Content)
	if err != nil {
		return err
	}
	return nil
}

func tileFetcher(inputPipe chan Tile, tilePipe chan Tile, maptype int) {
	url_format := MAPTYPES[maptype]
	for {
		tile := <-inputPipe
		tileObj := fetchTile(tile.z, tile.x, tile.y, url_format)
		tilePipe <- tileObj
	}
}

func fetchTile(z, x, y int, url_format string) Tile {
	tile := Tile{}
	tile_url := getTileUrl(z, x, y, url_format)
	resp, err := http.Get(tile_url)
	if err != nil {
		log.Fatal("Error in fetching tile")
	}
	defer resp.Body.Close()
	tile.x = x
	tile.z = z
	tile.y = y
	tile.Content, err = ioutil.ReadAll(resp.Body)
	return tile
}

func getTileUrl(z, x, y int, url_format string) string {
	// url_format = "http://mt2.google.com/vt/lyrs=y&x={x}&y={y}&z={z}"
	tile_url := strings.Replace(url_format, "{x}", strconv.Itoa(x), -1)
	tile_url = strings.Replace(tile_url, "{y}", strconv.Itoa(y), -1)
	tile_url = strings.Replace(tile_url, "{z}", strconv.Itoa(z), -1)
	return tile_url
}

func prepareDatabase(filename string) (*sql.DB, error) {
	os.Remove(filename)
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}

	err = optimizeConnection(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func setupMBTileTables(db *sql.DB, proj *Projection) error {

	_, err := db.Exec("create table if not exists tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
	if err != nil {
		return err
	}

	_, err = db.Exec("create table if not exists metadata (name text, value text);")
	if err != nil {
		return err
	}

	_, err = db.Exec("create unique index name on metadata (name);")
	if err != nil {
		return err
	}

	_, err = db.Exec("create unique index tile_index on tiles(zoom_level, tile_column, tile_row);")
	if err != nil {
		return err
	}

	// Load metadata.
	for name, value := range proj.MetaDataItems() {
		_, err := db.Exec("insert into metadata (name, value) values (?, ?)", name, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func optimizeConnection(db *sql.DB) error {
	_, err := db.Exec("PRAGMA synchronous=0")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA locking_mode=EXCLUSIVE")
	if err != nil {
		return err
	}
	_, err = db.Exec("PRAGMA journal_mode=DELETE")
	if err != nil {
		return err
	}
	return nil
}

func optimizeDatabase(db *sql.DB) error {
	_, err := db.Exec("ANALYZE;")
	if err != nil {
		return err
	}

	_, err = db.Exec("VACUUM;")
	if err != nil {
		return err
	}

	return nil
}

func main() {
	numCpus := runtime.NumCPU()
	log.Println("MbtileGo Version:", VERSION, "Number of CPUs:", numCpus)
	runtime.GOMAXPROCS(numCpus)
	var xmin, ymin, xmax, ymax float64
	var zoomlevel, maptype, max_zoomlevel int
	var filename string

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Println("Exit signal received.")
		log.Println(sig)
	}()

	flag.Float64Var(&xmin, "xmin", 55.397945, "Minimum longitude")
	flag.Float64Var(&xmax, "xmax", 55.402741, "Maximum longitude")
	flag.Float64Var(&ymin, "ymin", 25.291090, "Minimum latitude")
	flag.Float64Var(&ymax, "ymax", 25.292889, "Maximum latitude")
	flag.StringVar(&filename, "filename", "ouputFile.mbtile", "Output file to generate")
	flag.IntVar(&zoomlevel, "zoomlevel", 19, "Zoom level")
	flag.IntVar(&maptype, "maptype", 0, "0 for Google, 1 for OSM, 2 for mapbox satellite street")
	flag.IntVar(&max_zoomlevel, "max_zoomlevel", MAX_ZOOM_LEVEL, "Maximum zoomlevel to which tiles should be added")
	flag.Parse()

	proj := NewProjection(xmin, ymin, xmax, ymax, zoomlevel, max_zoomlevel, maptype)
	tiles := proj.TileList()
	if len(tiles) == 0 {
		log.Println("Not enough number of tiles. Please give proper bounds.")
		os.Exit(1)
	} else {
		log.Println("Filename: ", filename, " Zoom level ", zoomlevel, "-", max_zoomlevel, "  Number of tiles ", len(tiles))
	}


	db, err := prepareDatabase(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = setupMBTileTables(db, proj)
	if err != nil {
		log.Fatal(err)
	}

	inputPipe := make(chan Tile, len(tiles))
	tilePipe := make(chan Tile, len(tiles))
	outputPipe := make(chan Tile, len(tiles))

	for w := 0; w < 20; w++ {
		go tileFetcher(inputPipe, tilePipe, maptype)
	}

	for w := 0; w < 1; w++ {
		go mbTileWorker(db, tilePipe, outputPipe)
	}

	for _, tile := range tiles {
		inputPipe <- tile
	}

	// Waiting to complete the creation of db.
	for i := 0; i < len(tiles); i++ {
		<-outputPipe
	}

	err = optimizeDatabase(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Generated ", filename)

}

type Projection struct {
	Bc, Cc, Ac             []float64
	Zc                     [][]float64
	levels                 []int
	xmin, ymin, xmax, ymax float64
	metaData               MetaData
}

func NewProjection(xmin, ymin, xmax, ymax float64, zoomlevel, max_zoomlevel int, maptype int) *Projection {
	proj := Projection{xmin: xmin, ymin: ymin, xmax: xmax, ymax: ymax}
	for i := zoomlevel; i <= max_zoomlevel; i++ {
		proj.levels = append(proj.levels, i)
	}

	var e float64
	var c = float64(DEFAULT_TILE_SIZE)
	for i := 0; i <= max_zoomlevel; i++ {
		e = c / 2.0
		proj.Bc = append(proj.Bc, c/360.0)
		proj.Cc = append(proj.Cc, c/(2.0*math.Pi))
		proj.Zc = append(proj.Zc, []float64{e, e})
		proj.Ac = append(proj.Ac, c)
		c = c * 2
	}
	bounds := fmt.Sprintf("%f,%f,%f,%f", xmin, ymin, xmax, ymax)
	proj.metaData = NewMetaData(MAP_IMAGE_TYPES[maptype], zoomlevel, max_zoomlevel, bounds)
	return &proj
}

func (proj *Projection) project_pixels(x, y float64, zoom int) []float64 {
	d := proj.Zc[zoom]
	e := Round(d[0] + x*proj.Bc[zoom])
	f := minMax(math.Sin(DEG_TO_RAD*y), -0.9999, 0.9999)
	g := Round(d[1] + 0.5*math.Log((1+f)/(1-f))*-proj.Cc[zoom])
	return []float64{e, g}
}

func (proj *Projection) TileList() []Tile {
	var tilelist []Tile

	for _, zoom := range proj.levels {
		two_power_zoom := math.Pow(2, float64(zoom))
		px0 := proj.project_pixels(proj.xmin, proj.ymax, zoom) // left top
		px1 := proj.project_pixels(proj.xmax, proj.ymin, zoom) // right bottom
		xrangeStart := int(px0[0] / DEFAULT_TILE_SIZE)
		xrangeEnd := int(px1[0] / DEFAULT_TILE_SIZE)
		for x := xrangeStart; x <= xrangeEnd; x++ {
			if x < 0 || float64(x) >= two_power_zoom {
				continue
			}
			yrangeStart := int(px0[1] / DEFAULT_TILE_SIZE)
			yrangeEnd := int(px1[1] / DEFAULT_TILE_SIZE)
			for y := yrangeStart; y <= yrangeEnd; y++ {
				if y < 0 || float64(y) >= two_power_zoom {
					continue
				}
				// y = (int(two_power_zoom) - 1) - y
				tilelist = append(tilelist, Tile{z: zoom, x: x, y: y})
			}
		}

	}
	return tilelist
}

func (proj *Projection) MetaDataItems() map[string]string {
	return proj.metaData.Items()
}

type MetaData struct {
	tileFormat  string
	name        string
	description string
	minZoom     string
	maxZoom     string
	bounds      string
	_type       string
	version     string
}

func NewMetaData(tileFormat string, minZoom int, maxZoom int, bounds string) MetaData {
	uid := uuid.NewV4()
	metaData := MetaData{
		tileFormat:  tileFormat,   // required
		name:        uid.String(), // required
		description: uid.String(), // required
		minZoom:     strconv.Itoa(minZoom),
		maxZoom:     strconv.Itoa(maxZoom),
		bounds:      bounds,
		version:     MBTILE_VERSION, // required
		_type:       "overlay",      // required
	}
	return metaData
}

func (metaData MetaData) TileExtension() string {
	if metaData.tileFormat == PNG_IMAGE_FORMAT {
		return PNG_EXTENSION
	} else if metaData.tileFormat == JPG_IMAGE_FORMAT {
		return JPG_EXTENSION
	}
	return PNG_EXTENSION
}

func (metaData MetaData) TileFormat() string {
	return metaData.tileFormat
}

func (metaData MetaData) Version() string {
	return metaData.version
}

func (metaData MetaData) Items() map[string]string {
	data := map[string]string{
		"name":        metaData.name,
		"description": metaData.description,
		"format":      metaData.TileExtension(),
		"version":     metaData.version,
		"type":        metaData._type,
		"bounds":      metaData.bounds,
		"minzoom":     metaData.minZoom,
		"maxzoom":     metaData.maxZoom,
	}
	return data
}

func minMax(a, b, c float64) float64 {
	max_of_a_b := math.Max(a, b)
	return math.Min(max_of_a_b, c)
}

func Round(value float64) float64 {
	if value < 0 {
		return math.Ceil(value - 0.5)
	}
	return math.Floor(value + 0.5)
}
