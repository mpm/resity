require 'spec_helper'

describe Resity::Container do

  it 'does something useful' do
    expect(false).to eq(true)
  end

  before(:each) do
    # @file = StringIO.new("", "a+b")
    @file = Tempfile.new('barbtest')
    stub_const("Barb::Aggregator::OrderbookContainer::MAX_CHANGESETS", 10)
  end

  describe "#new" do
    it "writes header if no file exists" do
      ob = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      @file.seek(0)

      header = Barb::Aggregator::StorageHeader.new
      header.read(@file)
      header.name.should == 'test_btcusd'
      @file.length.should == 1024
    end

    it "breaks if wrong format" do
      header = Resity::Frames::StorageHeader.new
      header.version = 40
      header.write(@file)
      @file.seek(0)

      expect { ob = Container.new('test_btcusd', io: @file) }.to raise_exception(Barb::Aggregator::StorageError)
    end
  end

  describe "#add_snapshot" do
    it "creates a checkpoint" do
      ob = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      book = { 
               bids: { 110.0 => 2.1, 111.0 => 2.2 },
               asks: { 300.0 => 1.4, 301.0 => 0.008 }
             }
      ob.add_snapshot(Time.now, book)

      @file.seek(0)
      ob2 = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)

      ob2.bids.should == {110.0 => 2.1, 111.0 => 2.2 }
      ob2.asks.should == { 300.0 => 1.4, 301.0 => 0.008 }
    end

    it "creates one checkpoint and multiple diff sets" do
      ob = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      book = { 
               bids: { 110.0 => 2.1, 111.0 => 2.2 },
               asks: { 300.0 => 1.4, 301.0 => 0.008 }
             }
      t = Time.now
      ob.add_snapshot(t, book)
      ob.add_snapshot(t + 10, book)
      ob.add_snapshot(t + 20, book.merge({ bids: { 110.0 => 2.1 }}))

      ob2 = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      ob2.bids.should == { 110.0 => 2.1, 111.0 => 0.0 }
      ob2.asks.should == { 300.0 => 1.4, 301.0 => 0.008 }
    end

    it "creates another checkpoint if MAX_CHANGESETS changesets have been stored" do
      ob = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      book = { 
               bids: { 110.0 => 2.1, 111.0 => 2.2 },
               asks: { 300.0 => 1.4, 301.0 => 0.008 }
             }
             t = Time.now
      52.times do
        ob.add_snapshot(t, book)
      end
      ob2 = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      ob2.bids.should == { 110.0 => 2.1, 111.0 => 2.2 }
      ob2.asks.should == { 300.0 => 1.4, 301.0 => 0.008 }
    end

    describe "pointers" do
      before(:each) do

        ob = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
        book = { 
          bids: { 110.0 => 2.1, 111.0 => 2.2 },
          asks: { 300.0 => 1.4, 301.0 => 0.008 }
        }
        t = Time.now
        21.times do |i|
          ob.add_snapshot(t + i, book)
        end

        @ob2 = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      end

      it "sets next_block pointer to EOF+1 if last checkpoint" do
        @ob2.io.seek(@ob2.header.last_checkpoint)
        cp = Barb::Aggregator::CheckpointHeader.new
        cp.read(@ob2.io.read)
        # FIXME: laenge berechnen: size - 10x record length - obh - obr
        # oder so
        expect(cp.previous_block).to eq(0)
        expect(cp.next_block).to eq(@ob2.io.size + 1)
      end

      it "updates next_block pointer in previous block when adding a new checkpoint" do
        @ob2.io.seek(1024)
        cp = Barb::Aggregator::CheckpointHeader.new
        cp.read(@ob2.io.read)
        expect(cp.previous_block).to eq(0)
        # FIXME: das es hier einen fehler gibt ist ok.
        # laenge ausrechnen: headersize + cpsize + orh.size + obh x 10
        # oder so
        expect(cp.next_block).to eq(1024 + @ob2.io.size + 1)
        expect(cp.previous_block).to eq(0)
      end

      it "updates prev_block pointer in current block when adding a checkpoint" do

      end
    end
  end

  describe "#at_timestamp" do
    it "scans multiple blocks" do
      ob = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      book = { bids: { 110.0 => 5 }, asks: {} }
       t = Time.now
      53.times do |o|
        ob.add_snapshot(t + o * 10, book)
      end
      ob2 = Barb::Aggregator::OrderbookContainer.new('test_btcusd', io: @file)
      ob2.seek_timestamp(t + 51 * 10 + 3)
      ob2.bids.should == { 110.0 => 5 }
      ob2.last_timestamp.to_i.should == (t + 51 * 10).to_i
    end
  end
end

