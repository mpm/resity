require 'spec_helper'

describe Resity::Format::OrderBook do
  let(:format) { Resity::Format::OrderBook.new }
  let(:book) do 
    { bids: { 
      100.0 => 1.0,
       99.0 => 2.1,
       98.0 => 1.5
    },
    asks: {
      101.0 => 1.0,
      102.0 => 1.5,
      103.0 => 0.5
    } } 
  end 

  let(:book2) do
    { bids: { 
      100.0 => 5.0,
       99.0 => 2.1,
    },
    asks: {
      101.0 => 0.0,
      103.0 => 0.5
    } } 

  end

  describe "#new" do
    it "initializes with empty order book" do
      expect(format.data).to eq({bids: {}, asks: {}})
    end
  end
      
  context "I/O" do
    let(:buffer) { Resity::Format::OrderBook.new }
    let(:file) { StringIO.new }

    describe "#read_snapshot" do
      it "overwrites existing data with new snapshot" do
        buffer.data = book2
        buffer.write_snapshot(file)
        file.seek(0)

        format.data = book
        format.read_snapshot(file)
        expect(format.data[:bids]).to eq({ 100.0 => 5.0, 99.0 => 2.1 })
        expect(format.data[:asks]).to eq({ 101.0 => 0.0, 103.0 => 0.5 })
      end
    end

    describe "#read_delta" do
      it "applies delta to existing order book" do
        buffer.data = book
        buffer.data = book2
        buffer.write_delta(file)
        file.seek(0)

        format.data = book
        format.read_delta(file)
        expect(format.data[:bids]).to eq({ 100.0 => 5.0, 99.0 => 2.1, 98.0 => 1.5 })
        expect(format.data[:asks]).to eq({ 101.0 => 0.0, 102.0 => 1.5, 103.0 => 0.5 })
      end
    end

    describe "#write_snapshot" do
      it "writes complete order book" do
        format.data = book
        format.data = book2
        format.write_snapshot(file)

        file.seek(0)
        buffer.read_snapshot(file)
        expect(buffer.data[:bids]).to eq(book2[:bids])
        expect(buffer.data[:asks]).to eq(book2[:asks])
      end
    end

    describe "#write_delta" do
      it "writes delta from old and new order book" do
        # FIXME: einfach plain den inhalt des buffers ueberpruefen, aders
        # gehts wohl nicht
        format.data = book
        format.data = book2
        format.write_delta(file)

        file.seek(0)
        buffer.read_snapshot(file)
        expect(buffer.data[:bids]).to eq(book2[:bids])
        expect(buffer.data[:asks]).to eq(book2[:asks])
      end
    end
  end

  describe "#calc_delta" do

  end

  describe "#data" do
    it "returns current data" do
      format.data = book
      expect(format.data).to eq(book)
    end
  end

end
