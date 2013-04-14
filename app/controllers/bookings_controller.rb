class BookingsController < ApplicationController
  


  def index
    base = filter_out_nil_bookings.reverse_order(:booking_id).limit(100).all
    if params[:sort] and params[:direction]
       @bookings = base.sort_by {|record| record[params[:sort]]}
       @bookings.reverse! if params[:direction] == "desc"
    else
       @bookings = base
    end
  end

  def show
    @booking = Booking[params[:id]]
    @booked_items = filter_out_nil_fields_from_booked_items(params[:id])
  end

  private

  def filter_out_nil_bookings
    Booking.exclude(:consignee => nil)
  end

  def filter_out_nil_fields_from_booked_items(booking_number)
    #The purpose of this method is determine which fields are blank for all the booked items under a given booking number
    booked_items = Booking[booking_number].bking_item

    keys = booked_items[0].values.keys #get an array of the field_names, drawing from the first record

    blank_fields = [] #We'll store the names of the blank fields in this array

    keys.each do |key|
      column = booked_items.inject([]) {|collection,element| collection << element[key]} #Get all the values referenced by the current key and put them in a column array
      blank_fields << key if column.all? {|value| value.nil?} #put the current key in the blank field array if all values in that column are nil.
    end

    nonblank_fields = keys - blank_fields  #get the non-blank fields by subtracting the blank ones away

    output = []

    booked_items.each do |item|
      hash = {}
      item.values.each do |key,value|
        hash[key] = value if nonblank_fields.include? key
      end
      output << hash
    end

    return output
  end

end