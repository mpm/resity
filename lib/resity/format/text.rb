module Resity
  module Format

    # This class is used as an example. It can be used to store text
    # chaning over time, for example a wiki page or source code.
    # In practice you might want to use a more powerful engine like git
    # for this, but it will suffice to get the idea.
    class Text < Base

      require 'bindata'

      class TextHeader < ::BinData::Record
        endian :little
        uint32 :currency_conversion
        uint16 :bids_count
        uint16 :asks_count
      end

      class TextRecord < ::BinData::Record
        endian :little
        # FIXME: datatype
        double_le :amount
        double_le :price
      end
      attr_reader :last_data, :current_timestamp

      def initialize
        super
        reset
        @th = TextHeader.new
        @line = LineRecord.new
      end

    end
  end
end
