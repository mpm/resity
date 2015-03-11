module Resity
  class Container
    # number of diff sets, after there will be a new full snapshot stored,
    # max 65000 (16bit)
    MAX_CHANGESETS = 1000
    STORAGE_VERSION = 1

    attr_accessor :name, :bids, :asks, :last_timestamp, :format
    attr_reader :io, :header

    def initialize(filename, format, options = {})
      @name = filename
      @bids = {}
      @asks = {}
      @file = nil
      @io = options[:io]
      unless @io
        ff ="#{Barb::Application.root}/log/#{name}.aggr"
        unless File.exists?(ff)
          $logger.info "fresh db created at #{ff}"
          f = File.open(ff, 'w')
          f.close
        end
        @io = File.open(ff, "r+b")
        unless @io.flock(File::LOCK_EX)
          $logger.warn "could not lock db #{ff}"
        end
      end
      @last_timestamp = nil
      @last_checkpoint = nil

      @io.seek(0)
      if @io.size == 0
        # puts "create new db"
        initialize_file
      else
        # puts "load old db"
        initialize_from_file
        scan_last_timestamp unless options[:readonly]
      end
    end

    def seek_timestamp(timestamp)
      @io.seek(1024)
      while (scan_last_timestamp(timestamp) == :next_checkpoint) do

      end
    end

    def add_snapshot(timestamp, book)
      if @last_checkpoint == nil || @last_checkpoint.num_changesets >= MAX_CHANGESETS
        add_full_snapshot(timestamp, book)
      else
        add_diff(timestamp, book)
      end
    end


    def dump(options = {})
      recov = options[:recovery]
      w = 30
      puts "Header"
      puts "-" * w
      puts "name:            #{@header.name}"
      puts "version:         #{@header.version}"
      puts "type:            #{@header.type}"
      puts "last_checkpoint: #{@header.last_checkpoint}"
      puts "json_config:     #{@header.json_config}"
      puts
      puts "Checkpoints"
      puts "-" * w
      @io.seek(1024)
      ccount = 0
      cph = CheckpointHeader.new
      obh = OrderbookHeader.new
      obr = OrderbookRecord.new
      last_ts = 0
      while(@io.pos <= @header.last_checkpoint || recov) do
        cph.clear
        cph.read(@io)
        if recov && cph.num_changesets == 0
          print "WARN: recovering number of changesets: "
          current = @io.pos
          cs_count = 0
          obh.clear
          obh.read(@io)
          l = last_ts
          # while (obh.timestamp > lt && obh.currency_conversion == 0 && (obh.bids_count + obh.asks_count) > 0 && !@io.eof?) do
          while (obh.timestamp > l && obh.currency_conversion == 1 && (obh.bids_count + obh.asks_count) > 0 && !@io.eof?) do
            cs_count += 1
            l = obh.timestamp
            obh.clear
            obh.read(@io)
          end
          @io.seek(current)
          puts "probably #{cs_count}"
          cph.num_changesets = cs_count
          last_ts = obh.timestamp
        end
        puts "CP #{ccount += 1}:  #{cph.num_changesets} changesets"
        cph.num_changesets.times do |obh_seq|
          obh.clear
          obh.read(@io)
          puts " OBH #{obh_seq}: Time: #{Time.at(obh.timestamp / 1000.0)}. bids: #{obh.bids_count}, asks: #{obh.asks_count}"
          puts " WARN: timestamp dates to the past, compared to previous (#{Time.at(last_ts / 1000.0)}) changeset." if obh.timestamp < last_ts
          puts " WARN: timestamp set in the future." if obh.timestamp > Time.now.to_f * 1000
          print " "
          last_ts = obh.timestamp
          (obh.bids_count + obh.asks_count).times do
            obr.clear
            obr.read(@io)
            puts "  #{obr.price} #{obr.amount}"
            # print "."
          end
          print "\n"
        end
      end
      puts
      puts "Scan complete. Position: #{@io.pos}, filesize: #{@io.size}"
    end

    private

    def initialize_file
      @header = StorageHeader.new
      @header.version = STORAGE_VERSION
      @header.type = 20
      @header.name = @name
      @header.json_config = {}.to_json
      @header.last_checkpoint = 0
      write_header
    end

    def initialize_from_file
      @io.seek(0)
      @header = StorageHeader.new
      @header.read(@io)
      raise StorageError.new("incompatible container format: #{@header.version}") if @header.version > STORAGE_VERSION
    end


    def add_full_snapshot(timestamp, book)
      # TODO: update old checkpoint.
      prev_block = @last_checkpoint ? @header.last_checkpoint : 0
      
      # puts "add full snapshot"
      @io.seek(0, :END)
      @header.last_checkpoint = @io.pos

      cp = CheckpointHeader.new
      cp.previous_block = prev_block #TODO block pos!
      cp.next_block = 0
      cp.num_changesets = 0
      cp.write(@io)
      @last_checkpoint = cp
      
      @asks = {}
      @bids = {}
      add_diff(timestamp, book)
      # @bids = book[:bids]
      # @asks = book[:asks]
      # write_orderbook_records(timestamp, book)

      # @last_checkpoint.increase_changesets
      # update_last_checkpoint
      # @last_timestamp = timestamp

      write_header
      # puts "added snapshot. header points to #{@header.last_checkpoint}"
    end

    def add_diff(timestamp, book)
      @io.seek(0, :END)

      raise "last checkpoint unkown" unless @last_checkpoint

      updates_bids = Utils::Diff.updates(@bids, book[:bids])
      updates_asks = Utils::Diff.updates(@asks, book[:asks])

      write_orderbook_records(timestamp, { bids: updates_bids, asks: updates_asks })
      @last_checkpoint.increase_changesets
      update_last_checkpoint

      @bids = book[:bids]
      @asks = book[:asks]
      @last_timestamp = timestamp
    end

    # adds orderbook header + book records.
    def write_orderbook_records(timestamp, book)
      bids = book[:bids]
      asks = book[:asks]
      # puts "writing to orderbook: #{book.inspect}"

      obh = OrderbookHeader.new
      obh.bids_count = bids.size
      obh.asks_count = asks.size
      obh.timestamp = (timestamp.to_f * 1000).to_i # TODO
      obh.currency_conversion = 1
      obh.write(@io)

      row = OrderbookRecord.new
      [bids, asks].each do |one_book|
        one_book.each do |price, amount|
          row.price = price
          row.amount = amount
          row.write(@io)
        end
      end
    end

    def scan_last_timestamp(target_timestamp = nil)
      if target_timestamp
        puts "scan_last_timestamp: sequential search for timestamp #{target_timestamp} from current position."
      else
        if @header.last_checkpoint == 0
          puts "checkpoint not stored. leaving empty book, waiting for incoming data to be added"
          @io.seek(1024)
          return
        else
          @io.seek(@header.last_checkpoint)
        end
      end

      cp = CheckpointHeader.new
      cp.read(@io)

      @last_checkpoint = cp

      # starting from a fresh checkpoint, so reset existing bids and asks
      @bids = {}
      @asks = {}

      ob = OrderbookHeader.new
      row = OrderbookRecord.new

      # puts "num changesets: #{cp.num_changesets}"
      cp.num_changesets.times do
        old_pos = @io.pos
        ob.read(@io)
        if target_timestamp && ob.timestamp > target_timestamp.to_f * 1000
          puts "closest timestamp to target: #{@last_timestamp} (target #{target_timestamp})"
          return :found
        end
        @last_timestamp = Time.at(ob.timestamp / 1000.0)
        # puts "bids/asks: #{ob.bids_count}/#{ob.asks_count}. ts: #{@last_timestamp}"
        ob.bids_count.times do
        row = OrderbookRecord.new
          row.read(@io)
          # FIXME: float == fishy
          @bids[row.price.to_f] = row.amount.to_f
        end
        ob.asks_count.times do
          row.read(@io)
          # FIXME: float == fishy
          @asks[row.price.to_f] = row.amount.to_f
        end
      end
      # sanity check: file pointer should now be at the EOF
      return :next_checkpoint if target_timestamp
    end

    def write_header
      @header.spare = ""
      padding = "B" * (1024 - @header.to_binary_s.length)
      @header.spare = padding
      old_pos = @io.pos
      @io.seek(0)
      @header.write(@io)
      @io.flush
      @io.seek([1024, old_pos].max)
    end

    def update_last_checkpoint
      old_pos = @io.pos
      @io.seek(@header.last_checkpoint)
      @last_checkpoint.next_block = @io.size + 1
      @last_checkpoint.write(@io)
      @io.flush
      @io.seek(old_pos)
    end
  end
end
