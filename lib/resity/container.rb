require 'json'
module Resity
  class Container
    # number of diff sets, after there will be a new full snapshot stored,
    # max 65000 (16bit)
    MAX_CHANGESETS = 1000
    STORAGE_VERSION = 1

    attr_accessor :name, :last_timestamp, :format
    attr_reader :io, :header, :logger

    def initialize(filename, format, options = {})
      @format = format.new
      raise "invalid format #{@format.class}" if !(format < Resity::Format::Base)
      @format = format.new
      @logger = options[:logger]
      @name = filename
      @bids = {}
      @asks = {}
      @file = nil
      @io = options[:io]
      unless @io
        ff = filename
        unless File.exists?(ff)
          logger.info "fresh db created at #{ff}" if logger
          f = File.open(ff, 'w')
          f.close
        end
        @io = File.open(ff, "r+b")
        unless @io.flock(File::LOCK_EX)
          logger.warn "could not lock db #{ff}" if logger
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
        raise 'huh?'
      end
    end

    def add_snapshot(timestamp, data)
      if @last_checkpoint == nil || @last_checkpoint.num_changesets >= MAX_CHANGESETS
        add_full_snapshot(timestamp, data)
      else
        add_diff(timestamp, data)
      end
    end



    private

    def initialize_file
      @header = Frames::StorageHeader.new
      @header.version = STORAGE_VERSION
      @header.type = 20
      @header.name = @name
      @header.json_config = {}.to_json
      @header.last_checkpoint = 0
      write_header
    end

    def initialize_from_file
      @io.seek(0)
      @header = Resity::Frames::StorageHeader.new
      @header.read(@io)
      raise StorageError.new("incompatible container format: #{@header.version}") if @header.version > STORAGE_VERSION
    end


    def add_full_snapshot(timestamp, book)
      # TODO: update old checkpoint.
      prev_block = @last_checkpoint ? @header.last_checkpoint : 0
      
      # puts "add full snapshot"
      @io.seek(0, :END)
      @header.last_checkpoint = @io.pos

      cp = Frames::CheckpointHeader.new
      cp.previous_block = prev_block #TODO block pos!
      cp.next_block = 0
      cp.num_changesets = 0
      cp.write(@io)
      @last_checkpoint = cp
      
      # store data?
      @io.seek(0, :END)
      @format.write_snapshot(@io, timestamp)
      post_write_handling(timestamp)
      write_header # nur wg block pointers?
    end

    def add_diff(timestamp, data)
      @io.seek(0, :END)

      raise "last checkpoint unkown" unless @last_checkpoint

      @format.update(data, timestamp)
      @format.write_delta(@io, timestamp)
      post_write_handling(timestamp)
    end

    def post_write_handling(timestamp)
      @last_checkpoint.increase_changesets
      update_last_checkpoint
      @last_timestamp = timestamp
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

      cp = Frames::CheckpointHeader.new
      cp.read(@io)

      @last_checkpoint = cp

      # starting from a fresh checkpoint, so reset existing bids and asks
      @format.reset

      ob = Resity::Frames::OrderbookHeader.new
      row = Resity::Frames::OrderbookRecord.new

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
