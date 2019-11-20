package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo"
	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "adminadmin"
	dbname   = "githubgo"
)

var db *sql.DB

func getUsers(c echo.Context) error {

	sqlStatement := `
	CREATE TABLE IF NOT EXISTS users (
		id int PRIMARY KEY,
		login varchar UNIQUE NOT NULL,
		html_url varchar NOT NULL,
		avatar_url varchar,
		type varchar,
		site_admin boolean NOT NULL
	)`
	_, err := db.Exec(sqlStatement)
	checkErr(err)
	go startParsing()
	return c.HTML(http.StatusOK, "<b>PARSING USER STARTED, MESSAGES WILL BE SEEN IN CONSOLE</b>")
}

func startParsing() {
	client := http.Client{
		Timeout: time.Duration(5 * time.Second),
	}
	nextLinkParser := regexp.MustCompile(`<.*?>`)
	var res *http.Response
	var err error
	var nextLink string
	for {
		if len(nextLink) == 0 {
			res, err = client.Get("https://api.github.com/users?per_page=100")
			checkErr(err)
			nextLink = nextLinkParser.FindStringSubmatch(res.Header.Get("Link"))[0]
			nextLink = nextLink[1 : len(nextLink)-1]
		} else {
			res, err = client.Get(nextLink)
			nextLink = nextLinkParser.FindStringSubmatch(res.Header.Get("Link"))[0]
			nextLink = nextLink[1 : len(nextLink)-1]
			checkErr(err)
		}
		limit, err := strconv.Atoi(res.Header.Get("X-RateLimit-Remaining"))
		checkErr(err)
		if limit <= 1 {
			reset, err := strconv.Atoi(res.Header.Get("X-RateLimit-Reset"))
			checkErr(err)
			sleepTime := int64(reset) - time.Now().Unix() + 1
			fmt.Println("Requests limited. Program will be continued in " + strconv.FormatInt(sleepTime, 10) + " ms")
			time.Sleep(time.Duration(sleepTime) * time.Second)
			fmt.Println("Parsing resumed")
		}
		defer res.Body.Close()
		body, err := ioutil.ReadAll(res.Body)
		checkErr(err)

		type User struct {
			ID        int    `json:"id"`
			Login     string `json:"login"`
			GitHubURL string `json:"html_url"`
			AvatarURL string `json:"avatar_url"`
			UserType  string `json:"type"`
			SiteAdmin bool   `json:"site_admin"`
		}
		var githubResponse []User
		err = json.Unmarshal(body, &githubResponse)
		checkErr(err)
		vals := make([]interface{}, 0, len(githubResponse)*reflect.TypeOf(User{}).NumField())
		sqlStr := "INSERT INTO users(id, login, html_url, avatar_url, type, site_admin) VALUES"
		for _, v := range githubResponse {
			sqlStr += "(?,?,?,?,?,?),"
			vals = append(vals, v.ID, v.Login, v.GitHubURL, v.AvatarURL, v.UserType, v.SiteAdmin)
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		sqlStr = ReplaceSQL(sqlStr, "?")
		stmt, err := db.Prepare(sqlStr)
		checkErr(err)
		_, err = stmt.Exec(vals...)
		checkErr(err)
	}
}

//ReplaceSQL is replacing ? with $n for postgres
func ReplaceSQL(old, searchPattern string) string {
	tmpCount := strings.Count(old, searchPattern)
	for m := 1; m <= tmpCount; m++ {
		old = strings.Replace(old, searchPattern, "$"+strconv.Itoa(m), 1)
	}
	return old
}

func main() {
	var err error
	e := echo.New()
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err = sql.Open("postgres", psqlInfo)
	checkErr(err)
	db.SetMaxIdleConns(100)
	err = db.Ping()
	checkErr(err)
	e.GET("/", getUsers)
	e.Logger.Fatal(e.Start(":1234"))
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
