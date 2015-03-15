module Resity
  class Diff
    def self.updates(old_book, new_book)
      d = new(old_book, new_book)
      d.updates
    end

    def initialize(old_book, new_book)
      # the first two don't need to be duped necesarrily,
      # but maybe do it for thread safety?
      @old_book = old_book.dup
      @new_book = new_book.dup

      @updates = new_book.dup

      @old_book.each do |price, amount|
        if @new_book[price] == nil
          if amount == 0
            @updates.delete(price)
          else
            @updates[price] = 0.0
          end
        end
      end

      @new_book.each do |price, amount|
        if @old_book[price] == amount
          @updates.delete(price)
        end
      end
    end

    def updates
      @updates
    end
  end
end
