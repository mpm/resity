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
      @changesets_to_read = 0
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
      self.last_timestamp = nil
      @last_checkpoint = nil

      unless options[:no_init]
        @io.seek(0)
        if @io.size == 0
          initialize_file
        else
          initialize_from_file
          scan_last_timestamp if @mode == :write
        end
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
      format.data
    end

    def add_delta(timestamp, data)
      write_delta_header(timestamp)
      format.write_delta(@io)
      push_location
      update_last_snapshot_header
      update_header
      pop_location
    end

    #private
    def write_delta_header(timestamp)
      # untested
      csh = Frames::ChangesetHeader.new
      csh.timestamp = (timestamp.to_f * 1000).to_i
      csh.write(@io)
    end

    def write_snapshot_header
      cph = Frames::CheckpointHeader.new(
        previous_block: @header.last_checkpoint,
        next_block: 0,
        checksum: 0,
        num_changesets: 1)
      cph.write(@io)
      @last_checkpoint = cph
    end

    def add_snapshot(timestamp, data)
      write_snapshot_header
      write_delta_header(timestamp)
      format.reset
      format.update(data)
      format.write_snapshot(@io)
    end

    def seek(timestamp)
      raise_unless :read
      goto_first_snapshot if !last_timestamp || timestamp < last_timestamp

      while last_timestamp < timestamp
        # TODO: read_snapshot or skip back
        #read_snapshot # automatically loads next snapshot and skips all diffs inbetween, regardless of current position in file
        read_changeset
        #while (scan_last_timestamp(timestamp) == :next_checkpoint) do
        #  raise 'huh?'
        #end
      end
    end

    def goto_first_snapshot
      @io.seek(1024)
      read_changeset
    end

    def read_snapshot_header
      ch = Frames::ChangesetHeader.new
      cph = Frames::CheckpointHeader.new
      cph.read(@io)
      #cph.num_changesets.times do |index|
        ## TODO: stop after first read, store global state (if next checkpoint is ocming up or if we are inbetween)
        #ch.clear
        #ch.read(@io)
        #self.last_timestamp = ch.timestamp
        #format.read_delta
      #end
    end

    def read_changeset
      if @changesets_to_read == 0
        read_snapshot_header
        format.reset
      end
      ch.clear
      ch.read(@io)
      self.last_timestamp = ch.timestamp
      format.read_delta
      @changesets_to_read -= 1
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
      last_timestamp = timestamp
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
      # FIXME: use push_location
      old_pos = @io.pos
      @io.seek(@header.last_checkpoint)
      @last_checkpoint.next_block = @io.size + 1
      @last_checkpoint.write(@io)
      @io.flush
      @io.seek(old_pos)
    end

    def push_location
      @locations.push(@io.pos)
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
