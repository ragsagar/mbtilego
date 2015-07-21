package main

import (
	"database/sql"
	"fmt"
	"github.com/j4/gosm"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
)

func GetTileList(xmin float64, ymin float64, xmax float64, ymax float64, zoomlevel int) ([]*gosm.Tile, error) {
	t1 := gosm.NewTileWithLatLong(xmax, ymax, zoomlevel)
	t2 := gosm.NewTileWithLatLong(xmin, ymin, zoomlevel)
	tiles, err := gosm.BBoxTiles(*t1, *t2)
	return tiles, err
}

func main() {
	runtime.GOMAXPROCS(2)
	xmin := 55.397945
	ymin := 25.291090
	xmax := 55.402741
	ymax := 25.292889
	nbtiles := math.Abs((float64(xmax))-float64(xmin)) + math.Abs(float64(ymax)-float64(ymin))
	fmt.Println("Nbtiles ", nbtiles)
	tiles, err := GetTileList(xmin, ymin, xmax, ymax, 18)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Number of tiles ", len(tiles))

	db, err := prepareDatabase()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = setupMBTileTables(db)
	if err != nil {
		log.Fatal(err)
	}

	inputPipe := make(chan *gosm.Tile, len(tiles))
	tilePipe := make(chan Tile, len(tiles))
	outputPipe := make(chan Tile, len(tiles))

	for w := 0; w < 20; w++ {
		go tileFetcher(inputPipe, tilePipe)
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

}

type Tile struct {
	z, x, y int
	Content []byte
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
	_, err := db.Exec("insert into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);", tile.z, tile.x, tile.y, tile.Content)
	if err != nil {
		return err
	}
	return nil
}

func tileFetcher(inputPipe chan *gosm.Tile, tilePipe chan Tile) {
	for {
		tile := <-inputPipe
		tileObj := fetchTile(tile.Z, tile.X, tile.Y)
		tilePipe <- tileObj
	}
}

func fetchTile(z, x, y int) Tile {
	tile := Tile{}
	tile_url := getTileUrl(z, x, y)
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

func getTileUrl(z, x, y int) string {
	url_format := "http://c.tile.openstreetmap.org/{z}/{x}/{y}.png"
	tile_url := strings.Replace(url_format, "{x}", strconv.Itoa(x), -1)
	tile_url = strings.Replace(tile_url, "{y}", strconv.Itoa(y), -1)
	tile_url = strings.Replace(tile_url, "{z}", strconv.Itoa(z), -1)
	return tile_url
}

func prepareDatabase() (*sql.DB, error) {
	os.Remove("./test.mbutil")
	db, err := sql.Open("sqlite3", "./test.mbutil")
	if err != nil {
		return nil, err
	}

	err = optimizeConnection(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func setupMBTileTables(db *sql.DB) error {

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
