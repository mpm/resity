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
      format.data = 'test'
      expect(format.data).to eq('test')
    end

    it 'stores previous data in last_data' do
      format.data = 120
      format.data = 125
      expect(format.data).to eq(125)
      expect(format.last_data).to eq(120)
    end

    it 'calls delta method' do
      format.data = 120

      expect(format).to receive(:calc_delta).with(120, 125)
      format.data = 125
    end
  end

end
