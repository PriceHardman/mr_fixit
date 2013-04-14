class BillsOfLadingController < ApplicationController


  def create
    @bill_of_lading = BillOfLading.new({:type => params[:type],:bl_number => params[:bl_number]})
    @server_side_save_directory = "saved_bills_of_lading" #by default, bl's will be saved in the saved_bills_of_lading directory in the application root.

    case @bill_of_lading.type
      when "carrier"
        #@make_bl_xlsx_file = BillOfLadingExport::export_carrier_bl({:bl_number => @bill_of_lading.bl_number, :path => @server_side_save_directory})

      when "rail"
        #@make_bl_xlsx_file = BillOfLadingExport::export_rail_bl({:bl_number => @bl_number, :path => @server_side_save_directory})
    end
  end

  def new
    @bill_of_lading = BillOfLading.new
  end
end