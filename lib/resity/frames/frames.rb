module Resity
  module Frames
    class StorageHeader < BinData::Record
      endian :little
      uint8  :version

      # storage types:
      # 0-19 reserved
      # 20 orderbook
      # 21 orderbook with currency conversion (config holds converted currency)
      uint8  :type
      uint64 :last_checkpoint
      stringz :name
      stringz :json_config
      stringz :spare

    end

    # full dataset is following
    class CheckpointHeader < BinData::Record
      endian :little
      # TODO some contanst for file recovery
      uint64 :previous_block
      uint64 :next_block
      uint8  :checksum 
      uint32  :num_changesets # numnber of diff sets, say OrderbookHeaders

      def increase_changesets
        # WARNING: += 1 does not work on these fields
        self.num_changesets = self.num_changesets + 1
      end
    end

  end
end
