require 'spec_helper'

describe Resity::Format do
  let(:format) { Resity::Format::Base.new }

  describe "#new" do
    it "initializes" do
      expect { Resity::Format::Base.new }.not_to raise_error
    end
  end

  describe "#data" do
    it "returns current data" do
      format.update('test')
      expect(format.data).to eq('test')
    end
  end

  describe "#update" do
    it 'stores previous data in last_data' do
      format.update(120)
      format.update(125)
      expect(format.data).to eq(125)
      expect(format.last_data).to eq(120)
    end

    it 'calls delta method' do
      format.update(120)

      expect(format).to receive(:calc_delta).with(120, 125)
      format.update(125)
    end

    it 'stores results from delta method in delta_data' do
      format.update(120)

      expect(format).to receive(:calc_delta).with(120, 125).and_return(5)
      format.update(125)
      expect(format.delta_data).to eq(5)
    end
  end
end
