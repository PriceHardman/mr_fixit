class BillOfLading #tableless model, not backed by a database table
  include ActiveModel::Validations #allows us to perform validations
  include ActiveModel::Conversion #gives us the persisted? method to indicate our model is not saved to a database
  extend ActiveModel::Naming

  #A bill of lading is created by the user filling out and submitting the form at /bills_of_lading/new
  #The parameters passed by that form are :type and :bl_number, indicating the type of bl (carrier or rail) and the document number.
  attr_accessor :type, :bl_number

  validates_presence_of :type, :with => /(rail|carrier)/
  validates_presence_of :bl_number


  def initialize(attributes = {})
    attributes.each do |name,value|
      send("#{name}=",value)
    end
  end

  def persisted?
    false
  end


end