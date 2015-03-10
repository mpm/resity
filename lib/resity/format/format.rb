require 'spec_helper'
module Resity
  module Format
    class Base
      attr_reader :last_data, :current_timestamp, :delta_data

      def initialize
      end

      def data
        @data
      end

      def data=(value)
        @delta = calc_delta(data, value)
        @last_data = data
        @data = value
      end

      private

      def calc_delta(old, new)

      end
      # private_class_method :read_snapshot, :read_delta, :write_snapshop, :write_delta, :data, :data=
    end
  end
end

require 'resity/format/order_book'
require 'resity/format/text'
