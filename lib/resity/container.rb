require 'json'
module Resity
  class ContainerModeError < StandardError; end
  class Container
    # number of diff sets, after there will be a new full snapshot stored,
    # max 65000 (16bit)
    MAX_CHANGESETS = 1000
    STORAGE_VERSION = 1

    attr_accessor :name, :last_timestamp, :format
    attr_reader :io, :header, :logger

    def initialize(filename, format, mode, options = {})
      unless %i(read write).include?(mode)
        raise ContainerModeError.new("Illegal mode specified (#{mode}). Should be read or write")
      end
      @mode = mode
      @locations_stack = []
      @format = format.new
      raise "invalid format #{@format.class}" if !(format < Format::Base)
      @format = format.new
      @logger = options[:logger]
      @name = filename
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
        initialize_file
      else
        initialize_from_file
        scan_last_timestamp if @mode == :write
      end
    end

    def scan_last_timestamp
      # TODO: 
    end


    def write(timestamp, data)
      raise_unless :write
      if @last_checkpoint == nil || @last_checkpoint.num_changesets >= MAX_CHANGESETS
        add_snapshot(timestamp, data)
      else
        add_delta(timestamp, data)
      end
    end

    def data
      @format.data
    end

    def add_delta(timestamp, data)
      write_delta_header(timestamp)
      @format.write_delta(@io)
      push_location
      update_last_snapshot_header
      update_header
      pop_location
    end

    #private
    def write_diff_header
      # untested
      csh = Frames::ChangesetHeader.new
      csh.timestamp = timestamp
      @io.write(csh)
      @format.write_diff
    end

    def xadd_snapshot
      # ....
    end

    def seek(timestamp)
      goto_first_snapshot if timestamp < current_timestamp

      while current_timestamp < timestamp
        read_snapshot # automatically loads next snapshot and skips all diffs inbetween, regardless of current position in file
        #while (scan_last_timestamp(timestamp) == :next_checkpoint) do
        #  raise 'huh?'
        #end
      end
    end

    def goto_first_snapshot
      @io.seek(1024)
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
      @header = Frames::StorageHeader.new
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

    def push_location
      @locations.push(@io.position)
    end

    def pop_location
      @io.seek(@locations.pop)
    end

    def raise_unless(required_mode)
      if @mode != required_mode
        raise ContainerModeError.new("container needs to be opened in #{required_mode} for this")
      end
    end
  end
end
