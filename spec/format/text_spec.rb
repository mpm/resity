require 'spec_helper'

module Resity
  module Format
    describe Text do
      let(:format) { Text.new }
      let(:lines) do
        {0 => "# version 0.9",
         1 => "",
         2 => "This is the first",
         3 => "draft of this text.",
         4 => "Not much work was done."}
      end

      let(:lines2) do
        {0 => "# version 1.0",
         1 => "",
         2 => "This is the second",
         3 => "draft of this text.",
         4 => "Some more work was done."}
      end

      describe "#new" do
        it "initializes with empty text" do
          expect(format.data).to eq({})
        end
      end

      describe "#update and calc_delta" do
        it 'assinging data calls calc_delta with these params (already covered in format_spec.rb)' do
          format.update(lines)

          expect(format).to receive(:calc_delta).with(lines, lines2)
          format.update(lines2)
        end

        it 'delta_data holds proper diff' do
          format.update(lines)
          format.update(lines2)
          expect(format.delta_data).to eq(
            {0 => "# version 1.0",
             2 => "This is the second",
             4 => "Some more work was done."})
        end
      end

      describe "#reset" do
        it 'generates an empty orderbook without delta' do
          format.update(lines)
          format.reset
          expect(format.data).to eq({})
        end
      end

      context "I/O" do
        let(:buffer) { Text.new }
        let(:file) { StringIO.new }

        describe "#read_snapshot" do
          it "overwrites existing data with new snapshot" do
            buffer.update(lines2)
            buffer.write_snapshot(file)
            file.seek(0)

            format.update(lines)
            format.read_snapshot(file)
            expect(format.data).to eq(lines2)
          end
        end

        describe "#read_delta" do
          it "applies delta to existing data" do
            buffer.update(lines)
            buffer.update(lines2)
            buffer.write_delta(file)
            file.seek(0)

            format.update(lines)
            format.read_delta(file)

            expect(format.data).to eq(lines2)
          end
        end

        describe "#write_snapshot" do
          it "writes complete text" do
            format.update(lines)
            format.update(lines2)
            format.write_snapshot(file)

            file.seek(0)
            buffer.read_snapshot(file)
            expect(buffer.data).to eq(lines2)
          end
        end

        describe "#write_delta" do
          xit "writes delta from old and new order book" do
            format.update(book)
            format.update(book2)
            format.write_delta(file)

            file.seek(0)
            buffer.read_snapshot(file)
            expect(buffer.data[:bids]).to eq({100.0 => 5.0, 98.0 => 0.0})
            expect(buffer.data[:asks]).to eq({101.0 => 0.0, 102.0 => 0.0})
          end
        end
      end

      describe "#calc_delta" do
        xit 'describe me'
      end

      describe "#data" do
        it "returns current data" do
          format.update(lines)
          expect(format.data).to eq(lines)
        end
      end
    end
  end
end

