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
        diff = new.dup
        old.each do |line_no, content|
          if new[line_no] == nil
            if content == nil
              diff.delete(line_no)
            else
              diff[line_no] = nil
            end
          #elsif content == new[line_no]
            #diff.delete(line_no)
          end
        end

        new.each do |line_no, content|
          diff.delete(line_no) if old[line_no] == content
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
        data.each do |line_number, line|
          @line = LineRecord.new
          @line.line_number = line_number
          @line.line = line
          @line.len = line.length
          @line.write(file)
        end
      end

      def read_text_records(file)
        @th.read(file)
        @th.line_count.times do
          @line = LineRecord.new
          @line.read(file)
          @data[@line.line_number] = @line.line
        end
      end
    end
  end
end
