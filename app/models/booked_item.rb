class BookedItem < Sequel::Model(:bking_item)
  extend ActiveModel::Naming

  self.no_primary_key #this table doesn't have a primary key

  # This table's "booking_id" field is the primary key of bking_xaction (i.e. :booking)
  many_to_one :bking_xaction, :key => :booking_id, :class => :Booking

  #self.no_primary_key #this table -- for reasons that will forever evade me -- does not have a primary key
end
