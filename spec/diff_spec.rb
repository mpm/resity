require 'spec_helper'

describe Resity::Diff do
  before(:each) do
    @old = { 100 => 3.1, 101 => 0.5, 102 => 1 }

  end

  describe "#updates" do
    it "returns empty list if both books are identical" do
      d = Resity::Diff.new(@old, @old)
      expect(d.updates).to eq({})
    end

    it "returns items that are only in new book" do
      d = Resity::Diff.new(@old, @old.merge({ 100.5 => 7}))
      expect(d.updates).to eq({ 100.5 => 7})
    end

    it "overwrites items that are updated in new book" do
      d = Resity::Diff.new(@old, @old.merge({ 100 => 4}))
      expect(d.updates).to eq({ 100 => 4})
    end

    it "removes items that are no longer in new book by setting amount to zero" do
      update = @old.dup
      update.delete(102)
      d = Resity::Diff.new(@old, update) 
      expect(d.updates).to eq({ 102 => 0.0})
    end

    it "removes items from old book if they are not present in new book and old amounts are zero" do
      @old[102] = 0.0
      update = @old.dup
      update.delete(102)
      d = Resity::Diff.new(@old, update) 
      expect(d.updates).to eq({})
    end

    it "creates duplicates, so new_book is not modified" do
      update = @old.dup
      update[100] = 3.2
      d = Resity::Diff.new(@old, update) 
      expect(d.updates).to eq({ 100 => 3.2})
      expect(update).to eq({ 100 => 3.2, 101 => 0.5, 102 => 1 })
    end
  end

  describe "::updates" do
    it "instantiates a class and calls returns updates" do
      d = Resity::Diff.updates(@old, @old.merge({ 100 => 4}))
      expect(d).to eq({ 100 => 4})
    end
  end

end
