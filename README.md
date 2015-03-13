Resity
======

Really Simple Time Series Storage

## About

Resity is a simple file system based storage for timestamped timestamped
data of any kind.

Goal is to have a space efficient (it uses delta compression) storage with
high write perfomance. Similar to sqlite, no server process is needed.

Files containing a time series can be easily replicated with rsync.

### Motivation

Resity was developed to store order book data from financial markets (in this
case, crypto currencies).

An order book is a table containing the number of units (of the asset the
market is for) for purchase or sale at a specific price point.

This table is changing over time and these changes should be recorded in a
way that makes it possible to replay or jump to a specific timestamp and
reconstruct what the order book looked like at the time.

Traditionally, time series databases support only storing a single data
type per source (like a number) instead of a whole table.
Storing the order book table in a relational database would incur an
unnecessary overhead, since deleting or changing existing data is not
needed.

Resity was extracted from a software for logging market activity. Logging
would occur on a remote server but log files needed to be transfered to
local machine for further analysis.

With Resity, data is stored in a single file that grows like a log file
and can thus be easily and efficiently replicated with rsync.

### Other solutions

This gem is probably not for you if you don't have the exact same use case.
If you are looking to store time series based data, check out some mature
and more enterprisey tools for this like OpenTSDB and InfluxDB.

Depending on your data retention requirements, RRDTool might also be
interesting to you.

### The name

To my knowledge, Resity is not an existing word. It's based on ReSiTi
(Really Simple Time Series) Storage. Took the freedom the change the
last i to an y to make sound more english.

## Architecture

Resity provides a container that stores either full snapshots or delta frames
market with a time stamp.

Generating and parsing of these frames is specific to the data that should be
stored.

The gem provides two adaptors:

OrderBookFormat to store order book data as explained above

TextFormat to store changing text (for example wiki pages or source code).
This format is mainly for demonstration.

Custom formats can be implemented by subclassing Resity::Format

## Usage


### Installation

Add the following to your Gemfile:

    gem 'resity'

Then run:

    bundle

### Storing and retrieving data

Create a new container with 

container = Resity::Container.new('test.resdb', Resity::Format::OrderBook)

Operations are similar to File:

container.seek(Time.now - 1.hour) # seeks to the next dataset closest to given timestamp

container.read # returns an Resity::Format snapshot from the current position

container.write(data) # Passes data on to the Resity::Format object

The underlying container file will only be opened (and locked) while beeing accessed through seek, read and write

### Implementing custom formats

By subclassing Resity::Format, custom data formats can be implemented. 

The format adapter needs to have the following methods:

```ruby
read_snapshot(file) # read a full snapshot
apply_delta(file) # read a delta and apply it to previously read data

write_snapshot(file) # write full snapshot
write_delta(file) # write delta generated from new data compared to last data set

data(data) # set data, should automatically generate a delta compared to your last dataset
data=() # get data, retrieve current buffered data
```
## Running the tests

Resity uses RSpec. To run the tests, use:

    rake spec
