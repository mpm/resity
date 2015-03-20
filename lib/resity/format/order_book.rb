module Resity
  module Format
    require 'bindata'
    #
    # if this is the first header block after a checkpoint header,
    # it contains the full orderbook. if subsequent headers, if contains a diff
    class OrderBookHeader < ::BinData::Record
      endian :little
      uint64 :timestamp
      uint32 :currency_conversion
      uint16 :bids_count
      uint16 :asks_count
    end

    class OrderBookRecord < ::BinData::Record
      endian :little
      # FIXME: datatype
      double_le :amount
      double_le :price
    end

    # Stores order book data. An order book consists of two tables (bid
    # and ask) holding offers for buying/selling an asset at a certain
    # price point.
    # In Ruby, those tables are mapped with a hash. The price is the key
    # and the value denominates units of assets.
    #
    # Example:
    # { bids: { 100 => 1, 99 => 2 },
    #   asks: { 101 => 2, 105 => 4 } }
    #
    # This would be input data for the data methods. 
    # Bid means the market offers 1 asset unit for 100 monetary units, 2 asset
    # units for 99 etc.
    class OrderBook < Base

      def initialize
        super
        clear_data
        @last_timestamp = nil
        @obh = OrderBookHeader.new
        @obr = OrderBookRecord.new
      end

      def read_snapshot(file)
        clear_data
        read_order_book(file)
      end

      def read_delta(file)
        read_order_book(file)
      end

      def write_snapshot(file, timestamp = nil)
        write_order_book_records(file, data, timestamp || Time.now)
      end

      def write_delta(file, timestamp = nil)
        write_order_book_records(file, delta_data, timestamp || Time.now)
      end

      private

      def clear_data
        @data = @last_data = { bids: {}, asks: {} }
      end

      def read_order_book(file)
        @obh.read(file)

        [[@obh.bids_count, :bids], [@obh.asks_count, :asks]].each do |row|
          (row[0]).times do
            @obr.clear
            @obr.read(file)
            update_book_at_price(@obr, row[1])
          end
        end
        # FIXME: was damit?
        # @last_timestamp = Time.at(@obh.timestamp / 1000.0)
        # @walking[:changesets_read] += 1
      end

      def update_book_at_price(obr, key)
        @data[key][obr.price.to_f] = obr.amount.to_f
      end

      def write_order_book_records(file, data, timestamp)
        bids = data[:bids]
        asks = data[:asks]
        # puts "writing to orderbook: #{book.inspect}"

        obh = OrderBookHeader.new
        obh.bids_count = bids.size
        obh.asks_count = asks.size
        obh.timestamp = (timestamp.to_f * 1000).to_i # TODO
        obh.currency_conversion = 1
        obh.write(file)

        row = OrderBookRecord.new
        [bids, asks].each do |one_book|
          one_book.each do |price, amount|
            row.price = price
            row.amount = amount
            row.write(file)
          end
        end
      end

      def calc_delta(old, new)
        {
          bids: Resity::Diff.updates(old[:bids], new[:bids]),
          asks: Resity::Diff.updates(old[:asks], new[:asks])
        }
      end
    end
  end
end
