# FL Outages Mapped

## Usage

You should be able to map the outages by simply running:

```
datasette fl_outages.db
```

## Work

The grunt work was done by [Simon Willison](https://github.com/simonw/scrape-florida-outages) but I was able to transform the 
data into something that Datasette can plot on a map.

I probably didn't need to all this work but I like parquet as a format and
then finally converting the formatted data into a db file that works with Datasette.

Preview.mp4
