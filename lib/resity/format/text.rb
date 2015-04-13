module Resity
  module Format

    # This class is used as an example. It can be used to store text
    # chaning over time, for example a wiki page or source code.
    # In practice you might want to use a more powerful engine like git
    # for this, but it will suffice to get the idea.
    class Text < Base
      class TextHeader < ::BinData::Record
        endian :little
        uint16 :line_count
      end

      class LineRecord < ::BinData::Record
        endian :little
        uint16 :line_number
        uint16 :len
        string :line, :read_length => :len
      end

      def initialize
        super
        reset
        @th = TextHeader.new
        @line = LineRecord.new
        reset
      end

      def reset
        @data = @last_data = {}
      end

      def calc_delta(old, new)
        diff = new
        old.each do |line_no, content|
          if !diff[line_no]
            diff[line_no] = nil
          elsif content == diff[line_no]
            diff.delete(line_no)
          end
          diff
        end
      end
    end
  end
end
