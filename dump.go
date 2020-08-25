package main

import (
	"context"
	"flag"
	"log"
	"net/http"
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
	suburb    = flag.String("suburb", "Pyrmont", "Suburb to search")
	postcode  = flag.String("postcode", "2009", "Postcode to search")
)

func main() {
	flag.Parse()
	if *apiKey == "" {
		log.Fatalf("--api_key flag required")
	}
	if *projectID == "" {
		log.Fatalf("--project_id flag required")
	}

	fetchTime := time.Now().UTC()

	ctx := context.Background()
	bq, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Could not create BigQuery client: %v", err)
	}
	ds := bq.Dataset(*dataset)
	dsm, err := ds.Metadata(ctx)
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		if err := ds.Create(ctx, nil); err != nil {
			log.Fatalf("Couldn't create dataset: %v", err)
		}
	} else if err != nil {
		log.Fatalf("Couldn't get dataset metadata: %v", err)
	}
	log.Printf("%+v", dsm)

	schema, err := bigquery.InferSchema(Row{})
	if err != nil {
		log.Fatalf("couldn't infer schema: %v", err)
	}
	schema = schema.Relax()

	t := ds.Table(*table)
	_, err = t.Metadata(ctx)
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		if err := t.Create(ctx, nil); err != nil {
			log.Fatalf("Couldn't create table: %v", err)
		}
	} else if err != nil {
		log.Fatalf("couldn't get table metadata: %v", err)
	}
	_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
		Name:   "Domain Listings",
		Schema: schema,
	}, "")
	if err != nil {
		log.Fatalf("couldn't update table metadata: %v", err)
	}

	var c http.Client
	dc := domain.NewClient(&c, *apiKey)

	rsr := domain.ResidentialSearchRequest{
		ListingType: "Rent",
		Locations: []domain.LocationFilter{
			{
				State:                     *state,
				Area:                      "",
				Region:                    "",
				Suburb:                    *suburb,
				PostCode:                  *postcode,
				IncludeSurroundingSuburbs: false,
			},
		},
	}
	listings, err := dc.SearchResidential(rsr)
	if err != nil {
		log.Printf("error searching domain for %+v: %v\n", rsr, err)
		return
	}
	ins := t.Inserter()
	rows := []Row{}
	for _, l := range listings {
		rows = append(rows, Row{
			FetchTime: fetchTime,
			Listing:   l.Listing,
		})
	}
	if err := ins.Put(ctx, rows); err != nil {
		log.Fatalf("could not insert to bigquery: %v", err)
	}
}

type Row struct {
	FetchTime time.Time
	Listing   domain.PropertyListing
}
