class Booking < Sequel::Model(:bking_xaction)
  extend ActiveModel::Naming

  #booking_id is the primary key
  set_primary_key [:booking_id]

  # booking_id is used as a foreign key in bking_item table
  one_to_many :bking_item, :key => :booking_id, :class => :BookedItem

end