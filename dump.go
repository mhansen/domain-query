package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/mhansen/domain"
	"google.golang.org/api/googleapi"
)

var (
	apiKey    = flag.String("domain_api_key", "", "Domain API key")
	projectID = flag.String("bigquery_project_id", "", "Google BigQuery Project ID")
	dataset   = flag.String("dataset", "domain", "BigQuery dataset ID")
	table     = flag.String("table", "listings_test", "BigQuery table ID")
	state     = flag.String("state", "NSW", "State to search")
)

func fetch(w http.ResponseWriter, r *http.Request) {
	if err := fetchInternal(r); err != nil {
		w.WriteHeader(500)
		log.Printf("%v", err)
		fmt.Fprintf(w, "/fetch failed: %v", err)
		return
	}
	log.Println("OK")
	fmt.Fprint(w, "OK")
}

func main() {
	flag.Parse()
	if *apiKey == "" {
		log.Fatalf("--domain_api_key flag required")
	}
	if *projectID == "" {
		log.Fatalf("--bigquery_project_id flag required")
	}
	log.Print("Fetch server started.")

	http.HandleFunc("/fetch", fetch)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}

func fetchInternal(r *http.Request) error {
	fetchTime := time.Now().UTC()

	ctx := context.Background()
	bq, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		return fmt.Errorf("Could not create BigQuery client: %v", err)
	}
	ds := bq.Dataset(*dataset)
	dsm, err := ds.Metadata(ctx)
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		if err := ds.Create(ctx, nil); err != nil {
			return fmt.Errorf("Couldn't create dataset: %v", err)
		}
	} else if err != nil {
		return fmt.Errorf("Couldn't get dataset metadata: %v", err)
	}
	log.Printf("%+v", dsm)

	schema, err := bigquery.InferSchema(Row{})
	if err != nil {
		return fmt.Errorf("couldn't infer schema: %v", err)
	}
	schema = schema.Relax()

	t := ds.Table(*table)
	_, err = t.Metadata(ctx)
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		if err := t.Create(ctx, nil); err != nil {
			return fmt.Errorf("Couldn't create table: %v", err)
		}
	} else if err != nil {
		return fmt.Errorf("couldn't get table metadata: %v", err)
	}
	_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
		Name:   "Domain Listings",
		Schema: schema,
	}, "")
	if err != nil {
		return fmt.Errorf("couldn't update table metadata: %v", err)
	}

	var c http.Client
	dc := domain.NewClient(&c, *apiKey)

	ins := t.Inserter()
	for _, suburb := range r.URL.Query()["suburb"] {
		rsr := domain.ResidentialSearchRequest{
			ListingType: "Rent",
			Locations: []domain.LocationFilter{
				{
					State:                     *state,
					Area:                      "",
					Region:                    "",
					Suburb:                    suburb,
					PostCode:                  "",
					IncludeSurroundingSuburbs: false,
				},
			},
		}
		listings, err := dc.SearchResidential(rsr)
		if err != nil {
			return fmt.Errorf("error searching domain for %+v: %v", rsr, err)
		}
		rows := []Row{}
		for _, l := range listings {
			rows = append(rows, Row{
				FetchTime: fetchTime,
				Listing:   l.Listing,
			})
		}
		if err := ins.Put(ctx, rows); err != nil {
			return fmt.Errorf("could not insert to bigquery: %v", err)
		}
	}
	return nil
}

type Row struct {
	FetchTime time.Time
	Listing   domain.PropertyListing
}
