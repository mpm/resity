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
        end
        diff
      end

      def write_snapshot(file)
        write_text_records(file, data)
      end

      def write_delta(file)
        write_text_records(file, delta_data)
      end

      def read_snapshot(file)
        reset
        read_text_records(file)
      end

      def read_delta(file)
        read_text_records(file)
      end

      private

      def write_text_records(file, data)
        @th.line_count = data.size
        @th.write(file)
        data.each do |no, line|
          @line.line_number = no
          @line.line = line
          @line.len = line.length
          @line.write(file)
        end
      end

      def read_text_records(file)
        @th.read(file)
        lines = {}
        @th.line_count.times do
          @line.read(file)
          lines[@line.line_number] = @line.line
        end
        update(lines)
      end
    end
  end
end
