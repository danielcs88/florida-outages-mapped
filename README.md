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

https://github.com/danielcs88/florida-outages-mapped/blob/6928a9099d21b5eb5f83385e0397e51bc7c95823/Preview.mp4
