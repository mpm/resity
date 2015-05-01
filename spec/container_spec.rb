require 'spec_helper'

module Resity
  describe Container do

    before(:each) do
      # @file = StringIO.new("", "a+b")
      @file = Tempfile.new('barbtest')
      stub_const("Container::MAX_CHANGESETS", 10)
    end

    after(:each) do
      @file.close
      @file.unlink
    end

    describe 'new specs' do
      describe '#new' do
        it 'requires read or write mode' do
          %i(read write).each do |mode|
            expect do
              Container.new('test_btcusd', Format::Text, mode, io: @file)
            end.to_not raise_error
          end

          expect do
            Container.new('test_btcusd', Format::Text, :illegal_mode, io: @file)
          end.to raise_exception(ContainerModeError)
        end
        it 'creates a new file if none exists'
        it 'opens an existing file and skips to the end'
      end

      describe '#write' do
        let(:container) do
          Container.new('test_btcusd', Format::Text, :write, io: @file)
        end

        let(:data) { {0 => 'some data'} }

        before(:each) do
          container.write(Time.now, {0 => 'my text'})
        end

        it 'raises an error if opened in read mode' do
          read_container = Container.new('test_btcusd', Format::Text, :read, io: @file)
          expect { read_container.write(Time.now, 'hi there') }.to raise_exception(ContainerModeError)
        end

        it 'adds a snapshot via format' do
          @file.seek(1024 + (Frames::CheckpointHeader.new.num_bytes + Frames::ChangesetHeader.new.num_bytes))
          format = Format::Text.new

          format.read_delta(@file)
          expect(format.data).to eq({0=>'my text'})
        end

        it 'adds a delta on subsequent calls' do
          expect(container).to receive(:add_delta)
          container.write(Time.now, {0 => 'my text2'})
        end

        it 'adds a fullsnapshot again after X amounts of deltas' do
          #expect(container).to receive(:add_delta).exactly(9).times
          9.times { |i| container.write(Time.now, {0 => "my text #{i}"}) }

          expect(container).to receive(:add_snapshot)
          container.write(Time.now, {0 => 'my final text'})
        end
      end

      describe '#add_snapshot' do
        let(:container) do
          Container.new('test_btcusd', Format::Text, :write, io: @file)
        end

        let(:data) { {0 => 'some data'} }

        it 'adds a delta header'

        it 'increases @last_checkpoint.num_changesets' do
          container.write(Time.now, data)
          container.add_delta(Time.now, data)
          expect { container.add_delta(Time.now, data) }.
            to change { container.last_checkpoint.num_changesets }.by(1)
        end
      end

      describe '#seek' do
        it 'raises an error if opened in write mode' do
          container = Container.new('test_btcusd', Format::Text, :write, io: @file)
          expect { container.seek(Time.now) }.to raise_exception(ContainerModeError)
        end

        it 'skips forward to the nearest data that is > timestamp'
        it 'skips backwards to the nearest data the is < timestamp'
      end

      describe '#read' do
        it 'reads data into format at given timestamp'
      end

      describe "#read_snapshot_header" do
        xit 'reads a timestamp and first changeset' do
        end
      end

    end

=begin
    describe "#new", focus: true do
      it ', Raises an error if no format class is given as format' do
        expect {
          Container.new('test_btcusd', Integer)
        }.to raise_exception

      end

      it "writes header if no file exists" do
        Container.new('test_btcusd', Format::OrderBook, io: @file)
        @file.seek(0)

        header = Frames::StorageHeader.new
        header.read(@file)
        expect(header.name).to eq('test_btcusd')
        expect(@file.length).to eq(1024)
      end

      it "breaks if wrong format" do
        header = Frames::StorageHeader.new
        header.version = 40
        header.write(@file)
        @file.seek(0)

        expect { Container.new('test_btcusd', Format::OrderBook, io: @file) }.to raise_exception(Resity::StorageError)
      end
    end

    describe "#add_snapshot" do
      it "creates a checkpoint" do
        ob = Container.new('test_btcusd', Format::OrderBook, io: @file)
        book = { 
          bids: { 110.0 => 2.1, 111.0 => 2.2 },
          asks: { 300.0 => 1.4, 301.0 => 0.008 }
        }
        ob.add_snapshot(Time.now, book)

        @file.seek(0)
        ob2 = Container.new('test_btcusd', Format::OrderBook, io: @file)

        expect(ob2.data[:bids]).to eq({110.0 => 2.1, 111.0 => 2.2})
        expect(ob2.data[:asks]).to eq({300.0 => 1.4, 301.0 => 0.008})
      end

      it "creates one checkpoint and multiple diff sets" do
        ob = Container.new('test_btcusd', Format::OrderBook, io: @file)
        book = { 
          bids: { 110.0 => 2.1, 111.0 => 2.2 },
          asks: { 300.0 => 1.4, 301.0 => 0.008 }
        }
        t = Time.now
        ob.add_snapshot(t, book)
        ob.add_snapshot(t + 10, book)
        ob.add_snapshot(t + 20, book.merge({ bids: { 110.0 => 2.1 }}))

        ob2 = Container.new('test_btcusd', Format::OrderBook, io: @file)
        expect(ob2.data[:bids]).to eq({ 110.0 => 2.1, 111.0 => 0.0 })
        expect(ob2.data[:asks]).to eq({ 300.0 => 1.4, 301.0 => 0.008 })
      end

      it "creates another checkpoint if MAX_CHANGESETS changesets have been stored" do
        ob = Container.new('test_btcusd', Format::OrderBook, io: @file)
        book = {
          bids: { 110.0 => 2.1, 111.0 => 2.2 },
          asks: { 300.0 => 1.4, 301.0 => 0.008 }
        }
        t = Time.now
        52.times do
          ob.add_snapshot(t, book)
        end
        ob2 = Container.new('test_btcusd', Format::OrderBook, io: @file)
        expect(ob2.data[:bids]).to eq({ 110.0 => 2.1, 111.0 => 2.2 })
        expect(ob2.data[:asks]).to eq({ 300.0 => 1.4, 301.0 => 0.008 })
      end

      describe "pointers" do
        before(:each) do

          ob = Container.new('test_btcusd', Format::OrderBook, io: @file)
          book = {
            bids: { 110.0 => 2.1, 111.0 => 2.2 },
            asks: { 300.0 => 1.4, 301.0 => 0.008 }
          }
          t = Time.now
          21.times do |i|
            ob.add_snapshot(t + i, book)
          end

          @ob2 = Container.new('test_btcusd', Format::OrderBook, io: @file)
        end

        it "sets next_block pointer to EOF+1 if last checkpoint" do
          @ob2.io.seek(@ob2.header.last_checkpoint)
          cp = Frames::CheckpointHeader.new
          cp.read(@ob2.io.read)
          # FIXME: laenge berechnen: size - 10x record length - obh - obr
          # oder so
          expect(cp.previous_block).to eq(0)
          expect(cp.next_block).to eq(@ob2.io.size + 1)
        end

        it "updates next_block pointer in previous block when adding a new checkpoint" do
          @ob2.io.seek(1024)
          cp = Frames::CheckpointHeader.new
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
        # FIXME: error: creating this stalles the tests.
        ob = Container.new('test_btcusd', Format::OrderBook, io: @file)
        book = { bids: { 110.0 => 5 }, asks: {} }
        t = Time.now
        53.times do |o|
          ob.add_snapshot(t + o * 10, book)
        end
        ob2 = Container.new('test_btcusd', Format::OrderBook, io: @file)
        ob2.seek_timestamp(t + 51 * 10 + 3)

        expect(ob2.data[:bids]).to eq({ 110.0 => 5 })
        expect(ob2.last_timestamp.to_i).to eq((t + 51 * 10).to_i)
      end
    end
=end
  end

end
