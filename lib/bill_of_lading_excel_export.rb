require 'informix'
require 'axlsx'

#This file contains the code that queries the database and organizes the data.

#This method takes an SQL query as a string and outputs an array of hashes, with each hash a row in the result set
#The keys in the hashes are the field names
module BillOfLadingExport
  def output_as_hash_array(query)
    output_array =[] #the hashes returned by the query -- each of which represents a record -- will go in this array
    Informix.connect('accounting','price','password') do |connection|
      connection.cursor(query) do |cur|
        cur.open
        cur.each_hash do |row|
          output_array << row
        end
      end
    end
    return output_array
  end
  
  ##############################################################################
  
  def get_rail_bl_header(bl_number)
    query ="SELECT *
        FROM rail_bl
        WHERE id = #{bl_number};"
  output_as_hash_array(query)
  end
  
  def get_rail_bl_detail(bl_number) 
    query ="
      SELECT rail_bl_item.rail_bl_id,
      rail_bl_item.stcc,
      offload_ex.descr ex_vessel,
      offload.booking_id ex_trip_number,
      (TRIM(vessel.vessel_name)||\" \"||offload.voyage_no) vessel_voyage,
      (rail_bl_item.offload_id||\"-\"||rail_bl_item.extension_id) offload_id,
      rail_bl_item.qty,
      rail_bl_item.unit,
      rail_bl_item.prod_rate_item_id,
      (TRIM(pri_size_v.descr2)||\"  \"||TRIM(pri_size_v.descr)) product,
      rail_bl_item.total_weight net_weight,
      rail_bl_item.pallet_count
      FROM rail_bl_item
      INNER JOIN offload ON rail_bl_item.offload_id = offload.id
      INNER JOIN offload_ex ON offload.ex_id = offload_ex.id
      INNER JOIN pri_size_v ON pri_size_v.offload_id = rail_bl_item.offload_id 
            AND pri_size_v.extension_id = rail_bl_item.extension_id 
            AND pri_size_v.prod_rate_item_id = rail_bl_item.prod_rate_item_id
      INNER JOIN vessel ON vessel.vessel_code = offload.vessel_id
      WHERE rail_bl_item.rail_bl_id = #{bl_number}
      ORDER BY rail_bl_item.offload_id;"
    data = output_as_hash_array(query)
    
    #get the unique offload id's. We will iterate over this collection to make the summary section of the BL
    offload_numbers = data.inject([]) {|collection,row| collection<<row["offload_id"]}.uniq
  
    return {:data =>data,:offload_numbers => offload_numbers}
  end
  
  
  #While header info (i.e. data in tally_sheet table) can be referenced by rail_bl_id, the actual tally data (i.e. data in tally_sheet_item) must be referenced by tally_id.
  def get_rail_tally_header(bl_number) 
    query ="
      SELECT tally_sheet.id tally_number,
  tally_sheet.ref_tally_id wireless_number,
  tally_sheet.source_type_id,
  tally_sheet.waybill_id,
  offload_ex.descr ex_vessel,
  tally_sheet.rail_bl_id,
  tally_sheet.storage_id,
  tally_sheet.release_id,
  (tally_sheet.offload_id||\"-\"||tally_sheet.extension_id) offload_number,
  tally_sheet.rail_pallet_cnt,
  tally_sheet.dunnage_plt_qty,
  tally_sheet.hand_stow_yn,
  tally_sheet.xaction_date,
  tally_sheet.tare_wt,
  tally_sheet.loadplan_id
  FROM tally_sheet
  INNER JOIN offload_ex ON offload_ex.id = tally_sheet.ex_id
  WHERE rail_bl_id = #{bl_number};"
  
    data = output_as_hash_array(query)
    return data
  end
  
  def get_rail_tally_product_summary(input)
    #  This method takes the array/hash output of the query in get_rail_tally_detail
    #  This method compiles a list of the products represented by the tally data and
    #  summarizes the total number of cases and pallets for each product type.
  
    products = input.inject([]) {|collection,row| collection << row["product"]}.uniq! #create a unique array of all the products in the data
    data = [] #This array will hold the summary data as a hash
    products.each do |product|
      rows_with_this_product = input.select {|record| record["product"]==product} #select records matching the product
  
      qty = rows_with_this_product.inject(0){|collection,record| collection + record["qty"].to_i} #add up the total number of cases
      units = rows_with_this_product[0]["units"] #presumably all instances of one product will be the same unit (e.g. BAG), so just get the first one.
      plts = rows_with_this_product.inject(0) do |collection,record|
        #only count those pallets whose "hand_stow_yn" field is nil. Those with a "Y" indicate handstowed.
        if record["hand_stow_yn"] #if the hand_stow_yn field is not nil, i.e. it has value "Y"
          collection #don't add to the total
        else
          collection+record["pallet_yn"].to_i #add it to the total
        end
      end
      description = product
      pcode = rows_with_this_product[0]["product_code"]
  
      hash = {:qty => qty, :units => units, :plts => plts, :description => description, :pcode => pcode} #each record
      data << hash
    end
    return data
  end
  
  def get_rail_tally_detail(tally_number)
    #Get the data for each tally number.
      query = "
          SELECT tally_sheet_item.line_no,
          tally_sheet_item.qty,
          tally_sheet_item.units,
          tally_sheet_item.product_temp,
          tally_sheet_item.weight,
          tally_sheet_item.weight_type,
          tally_sheet_item.prod_rate_item_id product_code,
          (TRIM(pri_size_v.descr2)||\"  \"||TRIM(pri_size_v.descr)) product,
          tally_sheet_item.pallet_yn,
          tally_sheet_item.hand_stow_yn,
          tally_sheet_item.pallet_id
          FROM tally_sheet_item
          INNER JOIN pri_size_v ON pri_size_v.offload_id = SUBSTRING(tally_sheet_item.bar_code_id FROM 3 FOR 5)
          AND pri_size_v.extension_id = SUBSTRING(tally_sheet_item.bar_code_id FROM 8 FOR 1)
          AND pri_size_v.prod_rate_item_id = tally_sheet_item.prod_rate_item_id
          WHERE tally_sheet_id = #{tally_number}
          ORDER BY tally_sheet_item.line_no;"
    data = output_as_hash_array(query)
    product_summary = get_rail_tally_product_summary(data)
    return {:data => data, :product_summary => product_summary}
  end
  
  
  
  
  
  ######################################################################################
  ######################################################################################
  ######################################################################################
  ######################################################################################
  ######################################################################################
  
  
  
  
  def get_carrier_bl_header(bl_number)
    query = "
  SELECT *
  FROM waybill
  WHERE id = #{bl_number};
    "
    data = output_as_hash_array(query)
    return data
  end
  
  def get_carrier_bl_detail(bl_number)
    query = "
  SELECT waybill_item.waybill_id,
  waybill_item.offload_id,
  waybill_item.extension_id,
  (waybill_item.offload_id||\"-\"||waybill_item.extension_id) routing_number,
  waybill_item.qty,
  waybill_item.units,
  waybill_item.pallet_count,
  waybill_item.weight_type,
  waybill_item.weight,
  TRIM(product_rate_item.descr2) product,
  TRIM(offload_prod_size.descr) product_size,
  vessel.vessel_name,
  offload.voyage_no,
  offload_ex.descr ex_vessel,
  offload.booking_id ex_voyage
  FROM waybill_item
  INNER JOIN offload ON waybill_item.offload_id = offload.id
  INNER JOIN vessel ON offload.vessel_id = vessel.vessel_code
  INNER JOIN offload_product ON offload_product.prod_rate_item_id = waybill_item.prod_rate_item_id
                AND offload_product.offload_id = waybill_item.offload_id
                AND offload_product.extension_id = waybill_item.extension_id
  INNER JOIN product_rate_item ON product_rate_item.product_code_id = offload_product.product_code_id
  INNER JOIN offload_prod_size ON offload_prod_size.id = offload_product.product_size_id
  INNER JOIN offload_ex ON offload_ex.id = waybill_item.ex_id
  WHERE waybill_item.waybill_id = #{bl_number}
  ORDER BY waybill_item.offload_id, waybill_item.extension_id, product_rate_item.descr2,offload_prod_size.descr;
    "
  
    data = output_as_hash_array(query)
  
    #get the unique offload id's. We will iterate over this collection to make the summary section of the BL
    offload_numbers = data.inject([]) {|collection,row| collection<<row["offload_id"]}.uniq
  
    return {:data => data, :offload_numbers => offload_numbers}
  end
  
  def get_carrier_tally_header(bl_number) 
    query = "
  SELECT tally_sheet.*,
  offload_ex.descr ex_vessel 
  FROM tally_sheet
  INNER JOIN offload_ex ON offload_ex.id = tally_sheet.ex_id
  WHERE waybill_id = #{bl_number}
  ORDER BY tally_sheet.offload_id;
    "
  
    data = output_as_hash_array(query)
    return data
  end
  
  
  
  def get_carrier_tally_product_summary(input)
    #  This method takes the array/hash output of the query in get_carrier_tally_detail
    #  This method compiles a list of the products represented by the tally data and
    #  summarizes the total number of cases and pallets for each product type.
  
    products = input.inject([]) {|collection,row| collection << ( row["product_description"] + "-" + row["product_size"] )}.uniq.sort #create a unique array of all the products in the data, sorted alphabetically
    data = [] #This array will hold the summary data as a hash
    products.each do |product|
      rows_with_this_product = input.select {|record| ( record["product_description"] + "-" + record["product_size"] )==product} #select records matching the product
  
      qty = rows_with_this_product.inject(0){|collection,record| collection + record["qty"].to_i} #add up the total number of cases
      units = rows_with_this_product[0]["units"] #presumably all instances of one product will be the same unit (e.g. BAG), so just get the first one.
      weight = rows_with_this_product.inject(0) {|collection,record| collection += record["weight"].to_i}
      description = product
      weight_type = rows_with_this_product[0]["weight_type"]
  
      hash = {:qty => qty, :units => units, :weight => weight, :description => description, :weight_type => weight_type}
      data << hash
    end
    return data
  end
  
  
  
  
  def get_carrier_tally_detail(tally_number)
    query = "
  SELECT tally_sheet_item.*,
  TRIM(pri_size_v.descr2) product_description,
  pri_size_v.descr product_size
  FROM tally_sheet_item
  INNER JOIN pri_size_v ON pri_size_v.offload_id = SUBSTRING(tally_sheet_item.bar_code_id FROM 3 FOR 5)
              AND pri_size_v.extension_id = SUBSTRING(tally_sheet_item.bar_code_id FROM 8 FOR 1)
              AND pri_size_v.prod_rate_item_id = tally_sheet_item.prod_rate_item_id
  WHERE tally_sheet_id = #{tally_number}
  ORDER BY tally_sheet_item.line_no;
    "
    data = output_as_hash_array(query)
    product_summary = get_carrier_tally_product_summary(data)
  
    return {:data => data, :product_summary => product_summary}
  end

  def cell(row,column) #accepts row and column numbers as fixnums. NOTE: indices start at 0, e.g. column=0 => column "A"
    cols = {}
    ("A".."Z").inject(0){|number,letter| cols[letter]= number+1} #makes a hash of letters and numbers e.g. {"A"=>1, ..., "Z"=>26}
    cols = cols.invert #reverses the key:value pairs, e.g. {1 =>"A",...,26=>"Z"}
    column_letter = cols[column+1] #get the corresponding letter for the input column number, add 1 to offset 0-indexing.
    row_number = (row+1).to_s #get the row number, added by one to offset indexing, as a string
    column_letter+row_number #returns the alpha-numeric cell as a string, e.g. cell(4,5) => "E5"
  end


  def self.export_rail_bl(args = {})
    bl_header = get_rail_bl_header(args[:bl_number])[0] #Queries the database for one of two datasets needed for the first sheet.
    bl_detail = get_rail_bl_detail(args[:bl_number])[:data] #Queries the database for the second of the two datasets
    offload_numbers = get_rail_bl_detail(args[:bl_number])[:offload_numbers] #An array of the offload numbers represented by the data.

    #Begin making the spreadsheet.
    Axlsx::Package.new do |p|
      p.workbook do |wb| #make an excel file



        wb.add_worksheet(:name => "Rail BL ##{args[:bl_number].to_s}") do |sheet| #make the first sheet: The rail BL itself




                                                                                  ########################################## Begin Formatting  ####################################################
                                                                                  #We're going to need to keep track of which row we're on, for formatting purposes. We'll keep a counter
          current_row = 0

          # We're going to have different formatting styles for different cells.
          bold_and_centered = sheet.styles.add_style(:b => true,:alignment =>{:horizontal => :center,:shrink_to_fit => true},:shrink_to_fit => true) #bold and centered
          bold = sheet.styles.add_style(:b => true) #just bold
          centered = sheet.styles.add_style(:alignment =>{:horizontal => :center,:shrink_to_fit => true})
          right = sheet.styles.add_style(:alignment =>{:horizontal => :right,:shrink_to_fit => true})
          summary_row = sheet.styles.add_style(:b => true, :alignment =>{:horizontal => :center,:shrink_to_fit => true},:border => {:outline => true,:style => :thick,:color => "000000",:edges => [:top]})


          define_singleton_method("add_row") do |values,styles,merge_ranges| #making each row, takes three arguments
                                                                             #add the content to the cells from values array and set the styles for each cell from styles array
            sheet.add_row values,:style => styles
            #merge the ranges
            merge_ranges.each do |range|
              sheet.merge_cells(sheet.rows[current_row].cells[range])
            end
            current_row+=1
          end


          ###########################################  End Formatting  ######################################################





          #We will be adding data to the first 10 columns, A-J

          #The first row
          add_row ["Rail Bill of Lading",nil,nil,nil,nil,nil,nil,nil,nil,nil],[bold_and_centered],[0..9]

          add_row [],[],[] #blank row

          #column 	A   B   C   D   E   F   			G
          add_row [nil,nil,nil,nil,nil,nil,"Coastal Transportation, INC.",nil,nil,nil], bold, [6..9]
          add_row [nil,nil,nil,nil,nil,nil,"4025 13th AVE W. Seattle, WA 98119-1350",nil,nil,nil], bold, [6..9]
          add_row [nil,nil,nil,nil,nil,nil,"PHONE: (206) 282-9979  FAX: (206) 283-9121",nil,nil,nil], bold, [6..9]

          3.times {|ignore_this| add_row [],[],[]}

          add_row ["Rail Document #:",nil,bl_header["id"],nil,nil,nil,"Origin:",bl_header["received_city"]+", "+bl_header["received_state"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..3,7..9]

          add_row ["Date:",nil,bl_header["release_date"],nil,nil,nil,"Rail Car:",bl_header["car_initial"]+"-"+bl_header["car_id"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..3,7..9]

          add_row [nil,nil,nil,nil,nil,nil,"Route:",bl_header["routing"],nil,nil],
                  [nil,nil,nil,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [7..9]

          add_row [],[],[]

          add_row ["STCC:",bl_detail[0]["stcc"]],
                  [right,bold_and_centered],
                  [] #no merges necessary



          #Make the table summarizing the data in the BL
          #First, record the row number of the first row of data, since we'll need to know this to make the border for the entire table
          #For each offload number in the data, do
          #Make two header rows, the first with offload_id,ex_vessel+ex_vessel_trip,and vessel_voyage,
          #the second with QTY, UNITS, PLTS, DESCRIPTION, PLT WT, NET WT, and GROSS WT

          header_box_start_row = current_row+2 #The row number of the datapoint; This row will be used to sum up the totals at the end

          offload_numbers.each do |this_offload_id|
            #filter the raw data to get the records with this offload number
            data = bl_detail.select {|rows| rows["offload_id"]==this_offload_id}

            header = data[0] 	#we'll pull the header data from the first row of the filtered data

            #make the header row
            add_row ["Offload:",header["offload_id"],"Ex:",header["ex_vessel"]+" "+header["ex_trip_number"],nil,nil,"CTI Vessel/Voyage:",nil,header["vessel_voyage"],nil],
                    [bold_and_centered,right,bold_and_centered,nil,nil,right,bold_and_centered,nil,nil],
                    [3..5,6..7,8..9]
            add_row ["QTY","UNITS","PLTS","DESCRIPTION",nil,nil,nil,"PLT WT","NET WT","GROSS WT"],#values
                    bold_and_centered, #style
                    [3..4] #cells to merge

            #now loop through each individual row of data and place it
            data.each do |row|
              add_row [row["qty"],row["unit"],row["pallet_count"],row["product"],nil,nil,nil,"=45*"+cell(current_row,2),row["net_weight"],"="+cell(current_row,7)+"+"+cell(current_row,8)],
                      centered, #style
                      [3..4] #cells to merge
            end
          end

          #Make the row of summary values, in bold, making sums from header_box_start_row to current-1 for columns 0,2,7,8,and 9
          add_row ["=SUM("+cell(header_box_start_row,0)+":"+cell(current_row-1,0)+")",nil,
                   "=SUM("+cell(header_box_start_row,2)+":"+cell(current_row-1,2)+")",nil,nil,nil,nil,
                   "=SUM("+cell(header_box_start_row,7)+":"+cell(current_row-1,7)+")",
                   "=SUM("+cell(header_box_start_row,8)+":"+cell(current_row-1,8)+")",
                   "=SUM("+cell(header_box_start_row,9)+":"+cell(current_row-1,9)+")",],
                  summary_row,
                  []

          #one blank row
          add_row [],[],[]

          #now add the door seal and ryan rec numbers
          add_row ["DOOR SEAL #1",nil,bl_header["door_seal1"],nil,"RYAN REC #1",nil,bl_header["ryan_rec1"],"Shippers Counts",nil,nil],
                  [right,nil,bold_and_centered,nil,right,nil,bold_and_centered,bold_and_centered,nil,nil],
                  [0..1,4..5,7..9]
          add_row ["DOOR SEAL #2",nil,bl_header["door_seal2"],nil,"RYAN REC #2",nil,bl_header["ryan_rec2"],nil,nil,nil],
                  [right,nil,bold_and_centered,nil,right,nil,bold_and_centered,nil,nil,nil],
                  [0..1,4..5]
          add_row [nil,nil,nil,nil,"RYAN REC #3",nil,bl_header["ryan_rec3"],nil,nil,nil],
                  [nil,nil,nil,nil,right,nil,bold_and_centered,nil,nil,nil],
                  [4..5]

          img = File.expand_path(Dir.pwd<<'/coastal.png', __FILE__)
          sheet.add_image(:image_src => img, :noSelect => true, :noMove => true, :hyperlink=>"http://coastaltransportation.com/") do |image|
            image.width = 196
            image.height = 75
            image.hyperlink.tooltip = "Thank You For Choosing Coastal Transportation!"
            image.start_at 1, 2
          end




          #make the columns on the sheet the approriate width
          sheet.column_widths 8.43,8.43,8.43,9.57,8.43,8.43,9.43,8.43,8.43,13.14
        end




        ##################################################################################################################################################
        ##################################################################################################################################################
        ##################################################################################################################################################
        ##################################################################################################################################################


        #Make a tally sheet for each offload number present in the rail car.
        tally_headers = get_rail_tally_header(args[:bl_number])

        #In other words, make an entire tally sheet for each record in tally_headers.
        tally_headers.each do |tally_header|

          #Make the sheet
          wb.add_worksheet(:name => "Tally for Offload #{tally_header["offload_number"]}") do |sheet|
            #Get the detail info by referencing the id of the header:
            tally_data = get_rail_tally_detail(tally_header["tally_number"])
            tally_detail = tally_data[:data]
            tally_summary = tally_data[:product_summary]

            #cetain vars used in the sheet that draw from the detail or header:
            number_of_dunnage_pallets = tally_header["dunnage_plt_qty"]


            current_row = 0 #initialize the counter

            # We're going to have different formatting styles for different cells.
            bold_and_centered = sheet.styles.add_style(:b => true,:alignment =>{:horizontal => :center,:shrink_to_fit => true},:shrink_to_fit => true) #bold and centered
            bold = sheet.styles.add_style(:b => true) #just bold
            centered = sheet.styles.add_style(:alignment =>{:horizontal => :center,:shrink_to_fit => true})
            right = sheet.styles.add_style(:alignment =>{:horizontal => :right,:shrink_to_fit => true})
            summary_row = sheet.styles.add_style(:b => true, :alignment =>{:horizontal => :center,:shrink_to_fit => true},:border => {:outline => true,:style => :thick,:color => "000000",:edges => [:top]})
            header_row = sheet.styles.add_style(:b => true, :sz => 10, :alignment => {:horizontal => :center,:shrink_to_fit => true})
            detail_cells = sheet.styles.add_style(:alignment =>{:horizontal => :center,:shrink_to_fit => true})

            define_singleton_method("add_row") do |values,styles,merge_ranges| #making each row, takes three arguments
                                                                               #add the content to the cells from values array and set the styles for each cell from styles array
              sheet.add_row values,:style => styles
              #merge the ranges
              merge_ranges.each do |range|
                sheet.merge_cells(sheet.rows[current_row].cells[range])
              end
              current_row+=1
            end

            #make the header rows

            #first row blank:
            add_row [],[],[]

            #rail BL# and tally #
            add_row ["RAIL BL #:",nil,tally_header["rail_bl_id"],nil,nil,nil,nil,"TALLY #:",tally_header["tally_number"],nil,nil],
                    [right,nil,bold_and_centered,nil,nil,nil,nil,right,bold_and_centered,nil,nil],
                    [0..1,2..4,8..10]
            #ex vessel and wireless number
            add_row ["EX VESSEL:",nil,tally_header["ex_vessel"],nil,nil,nil,nil,"WIRELESS #:",tally_header["wireless_number"],nil,nil],
                    [right,nil,bold_and_centered,nil,nil,nil,nil,right,bold_and_centered,nil,nil],
                    [0..1,2..4,8..10]
            #offload number and rail car number
            add_row ["OFFLOAD #:",nil,tally_header["offload_number"],nil,nil,nil,nil,"RAIL CAR:","=\'Rail BL ##{tally_header["rail_bl_id"]}\'!H10",nil,nil],
                    [right,nil,bold_and_centered,nil,nil,nil,nil,right,bold_and_centered,nil,nil],
                    [0..1,2..4,8..10]

            #blank row
            add_row [],[],[]

            #save the row number of the first row of summary tally data:
            first_row_of_tally_data = current_row+1

            #make the summary header row
            add_row ["LOAD","CASE CT","UNITS","TEMP","WEIGHT","TYPE","P. CODE","PRODUCT","PLTS","HS","PLT#"],
                    header_row, #all bold and centered
                    []    #no merged cells




            #MAKE THE TALLY DATA
            tally_detail.each do |row|
              add_row [row["line_no"],row["qty"],row["units"],row["product_temp"],row["weight"],row["weight_type"],row["product_code"],row["product"],row["pallet_yn"],row["hand_stow_yn"],row["pallet_id"]],
                      [detail_cells,detail_cells,detail_cells,detail_cells,detail_cells,detail_cells,detail_cells,nil,detail_cells,detail_cells,detail_cells],
                      []
            end





            #make the summary row:
            last_row_of_tally_data = current_row-1
            tally_summary_row = current_row
            case_count = cell(tally_summary_row,1)
            total_tallied_weight = cell(tally_summary_row,4)
            add_row [nil,
                     "=SUM("+cell(first_row_of_tally_data,1)+":"+cell(last_row_of_tally_data,1)+")",
                     nil,
                     nil,
                     "=SUM("+cell(first_row_of_tally_data,4)+":"+cell(last_row_of_tally_data,4)+")",
                     nil,
                     nil,
                     nil,
                     "=SUM("+cell(first_row_of_tally_data,8)+":"+cell(last_row_of_tally_data,8)+")",
                     "=COUNTIF("+cell(first_row_of_tally_data,9)+":"+cell(last_row_of_tally_data,9)+",\"Y\")",
                     nil],
                    summary_row,
                    []

            #add a blank row
            add_row [],[],[]

            #make the product summary section at the bottom:
            add_row ["PRODUCT SUMMARY",nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                    bold,
                    [0..3]
            add_row ["QTY","UNITS","PLTS","DESCRIPTION",nil,nil,"P. CODE",nil,nil,nil,nil],
                    bold_and_centered,
                    [3..5]

            start_of_product_summary_data = current_row
            #now, loop through the tally summary data and place it accordingly:
            tally_summary.each do |row|
              add_row [row[:qty],row[:units],row[:plts],row[:description],nil,nil,row[:pcode]],
                      detail_cells,
                      [3..5]
            end

            total_cases = cell(current_row,1)


            #now place the totals of the product summary data:
            add_row ["=SUM("+cell(start_of_product_summary_data,0)+":"+cell(current_row-1,0)+")",
                     nil,"=SUM("+cell(start_of_product_summary_data,2)+":"+cell(current_row-1,2)+")",
                     nil,
                     nil,
                     nil,
                     nil],
                    summary_row,
                    []

            #add three blank rows
            add_row [],[],[]
            add_row [],[],[]
            add_row [],[],[]

            #create the totals at the bottom of the sheet

            #total number of tallied pallets. Calculate by counting the number of lines in the tally:
            tallied_pallets = cell(current_row,7)
            add_row ["# OF TALLIED PALLETS:",nil,nil,nil,nil,nil,nil,"="+cell(last_row_of_tally_data+1,8)],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            #Total weight of tallied pallets. Calculated by multiplying the number of tallied pallets by 45 (i.e. pallet weight)
            add_row ["TOTAL WEIGHT OF TALLIED PALLETS:",nil,nil,nil,nil,nil,nil,"=45*#{tallied_pallets}"],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            number_of_handstowed_loads = cell(current_row,7)
            # Number of handstowed pallets. Calculated by taking the total number of tally records whose HS field reads "Y"
            add_row ["# OF HAND STOWED LOADS IN RAIL CAR:",nil,nil,nil,nil,nil,nil,"="+cell(last_row_of_tally_data+1,9)],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            number_of_pallets_in_rail_car = cell(current_row,7)
            # Number of pallets in rail car. Calculated as the # of tallied pallets minus the # of handstowed pallets
            add_row ["# OF PALLETS OF PRODUCT IN RAIL CAR:",nil,nil,nil,nil,nil,nil,"=#{tallied_pallets}-#{number_of_handstowed_loads}"],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            total_pallet_weight = cell(current_row,7)
            # Total weight of pallets in rail car. 45 times number of pallets in rail car
            add_row ["TOTAL PALLET WEIGHT IN RAIL CAR:",nil,nil,nil,nil,nil,nil,"=45*#{number_of_pallets_in_rail_car}"],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            number_of_dunnage_pallets_cell = cell(current_row,7)
            # Number of dunange pallets in rail car. Data is stored in the tally header.
            add_row ["# OF DUNNAGE PALLETS IN RAIL CAR:",nil,nil,nil,nil,nil,nil,number_of_dunnage_pallets],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            total_dunnage_weight = cell(current_row,7)
            # Total weight of dunnage pallets in rail car
            add_row ["TOTAL WEIGHT OF DUNNAGE PALLETS IN RAIL CAR:",nil,nil,nil,nil,nil,nil,"=45*#{number_of_dunnage_pallets_cell}"],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            net_product_weight = cell(current_row,7)
            # Net product weight in rail car. Calculated as total of weight column minus total pallet weight in rail car
            add_row ["NET PRODUCT WEIGHT IN RAIL CAR:",nil,nil,nil,nil,nil,nil,"="+total_tallied_weight+"-"+total_pallet_weight],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            # Gross total weight in rail car. Calculated as net product weight plus total pallet weight in rail car plus total dunnage pallet weight
            add_row ["GROSS WEIGHT IN RAIL CAR:",nil,nil,nil,nil,nil,nil,"="+net_product_weight+"+"+total_pallet_weight+"+"+total_dunnage_weight],
                    [right,nil,nil,nil,nil,nil,nil,bold_and_centered],
                    [0..6]

            sheet.column_widths 4.86,7.14,5.86,5.57,7.29,4.43,7.86,22.14,5.14,2.86,7.14
          end #of sheet
        end #of loop through offload numbers
      end
      p.serialize "#{args[:path].to_s}/Rail_BL_#{args[:bl_number].to_s}.xlsx" #save the file
    end
  end


########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################

  def self.export_carrier_bl(args = {})

    # Data for the excel spreadsheets comes from methods in bill_of_lading_excel_export.rb which query Marine Traffic
    # and return arrays of hashes containing field_name:value pairs.

    # get_carrier_bl_header returns the data for the header.
    # get_carrier_bl_detail[:data] returns the line items for the bl
    # get_carrier_bl_detail[:offload_numbers] returns an array of the offload numbers
    # represented in the bl. This list is iterated over when creating the spreadsheet.

    bl_header = get_carrier_bl_header(args[:bl_number])[0] #run the query for the header, which returns only one row. Return that row (index 0 of the array).
    bl_detail = get_carrier_bl_detail(args[:bl_number]) #run the query for the detail. This var isn't actually referenced in the creation process; It contains the :data and :offload_numbers arrays
    offload_numbers = bl_detail[:offload_numbers] #assign the offload_numbers a variable
    bl_data = bl_detail[:data] #assign the actual bl data



    #make the excel workbook, the contents of which will be: 1 sheet for the BL itself, and then one sheet for each offload-extension pair present on the BL (usually just one).
    Axlsx::Package.new do |p|
      p.workbook do |wb|
        wb.add_worksheet(:name => "Carrier BL ##{args[:bl_number].to_s}") do |sheet| #creates a sheet, the name of which is "Carrier BL #<number>", based on the user input.


                                                                                     ########################################## Begin Formatting  ####################################################
                                                                                     #We're going to need to keep track of which row we're on, for formatting purposes. We'll keep a counter
          current_row = 0

          # We're going to have different formatting styles for different cells.
          bold_and_centered = sheet.styles.add_style(:b => true,:alignment =>{:horizontal => :center,:shrink_to_fit => true})
          bold = sheet.styles.add_style(:b => true) #just bold
          centered = sheet.styles.add_style(:alignment =>{:horizontal => :center,:shrink_to_fit => true})
          right = sheet.styles.add_style(:alignment =>{:horizontal => :right,:shrink_to_fit => true})
          right_small = sheet.styles.add_style(:sz => 10,:alignment =>{:horizontal => :right,:shrink_to_fit => true})
          summary_row = sheet.styles.add_style(:b => true, :alignment =>{:horizontal => :center,:shrink_to_fit => true},:border => {:outline => true,:style => :thick,:color => "000000",:edges => [:top]})


          define_singleton_method("add_row") do |values,styles,merge_ranges| #making each row, takes three arguments
                                                                             #add the content to the cells from values array and set the styles for each cell from styles array
            sheet.add_row values,:style => styles
            #merge the ranges
            merge_ranges.each do |range|
              sheet.merge_cells(sheet.rows[current_row].cells[range])
            end
            current_row+=1
          end


          ###########################################  End Formatting  ######################################################

          #The first row
          add_row ["Trailer/Container Bill of Lading",nil,nil,nil,nil,nil,nil,nil,nil,nil],bold_and_centered,[0..9]

          add_row [],[],[] #blank row

          #column   A   B   C   D   E   F         G
          add_row [nil,nil,nil,nil,nil,nil,"Coastal Transportation, INC.",nil,nil,nil], bold, [6..9]
          add_row [nil,nil,nil,nil,nil,nil,"4025 13th AVE W. Seattle, WA 98119-1350",nil,nil,nil], bold, [6..9]
          add_row [nil,nil,nil,nil,nil,nil,"PHONE: (206) 282-9979  FAX: (206) 283-9121",nil,nil,nil], bold, [6..9]


          3.times {|ignore_this| add_row [],[],[]}

          add_row ["Trailer Document #:",nil,bl_header["id"],nil,nil,nil,"Shipper:",bl_header["shipper"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..4,7..9]

          #bl_header["creation_time"] is a datetime string, e.g. "2013-01-30 17:09"
          #We want just the date, so get only the date part using regex /\d+-\d+-\d+/
          add_row ["Date:",nil,bl_header["creation_date"].to_s,nil,nil,nil,"To:",bl_header["consignee"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..4,7..9]

          add_row ["Routing Number:",nil,bl_header["offload_id"]<<"-"<<bl_header["extension_id"],nil,nil,nil,"C/O:",bl_header["care_of"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..4,7..9]

          add_row ["Carrier:",nil,bl_header["carrier"],nil,nil,nil,"City:",bl_header["city"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..4,7..9]

          #make a lambda literal that, if a container prefix is given, will prepend that to the container id.
          #Otherwise only return the container id.
          container = ->(header) do #pass the bl_header array into the lambda
            if header["cont_prefix_id"] #if the container has a prefix
              header["cont_prefix_id"]<<" "<<header["container_id"] #put the prefix in front and return both it and container id
            else
              header["container_id"] #just return the container id
            end
          end
          add_row ["Container #:",nil,container.call(bl_header),nil,nil,nil,"State:",bl_header["state"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..4,7..9]

          add_row ["Seal #:",nil,bl_header["seal_id"],nil,nil,nil,"Country:",bl_header["country"],nil,nil],
                  [right,nil,bold_and_centered,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [0..1,2..4,7..9]

          add_row [nil,nil,nil,nil,nil,nil,"Zip Code:",bl_header["postal_code"],nil,nil],
                  [nil,nil,nil,nil,nil,nil,right,bold_and_centered,nil,nil],
                  [7..9]

          add_row [],[],[]

          #make a lambda literal to append "Fahrenheit" to the temperature only if temperature is not null. Otherwise a methodMissing error would be thrown:
          #
          append_temp_scale = ->(temperature){temperature.to_s<<" Fahrenheit" if temperature}

          add_row ["Maintain Temp. At:",nil,append_temp_scale.call(bl_header["core_temp"]),nil,nil,nil],
                  [right_small,nil,bold_and_centered,nil,nil],
                  [0..1,2..4]

          add_row ["Pallets Exhanged:",nil,bl_header["pallet_amt"],nil,nil,nil],
                  [right,nil,bold_and_centered,nil,nil],
                  [0..1,2..4]

          add_row [],[],[]

          #Add the CTI logo, anchored at row 1 column 2.
          img = File.expand_path(Dir.pwd<<'/coastal.png', __FILE__)
          sheet.add_image(:image_src => img, :noSelect => true, :noMove => true, :hyperlink=>"http://coastaltransportation.com/") do |image|
            image.width = 196
            image.height = 75
            image.hyperlink.tooltip = "Thank You For Choosing Coastal Transportation!"
            image.start_at 1, 2
          end

          #record the row number of the first row of summary data, so that we can summarize the results below
          first_row_of_bl_summary = current_row+2


          #For each offload number represented in the data:
          #1. Filter the data to include only data with that offload number
          #2. Select the first row of that filtered data as the header
          #3. Make a header row containing that header data
          #4. Make a row with column labels (qty,units,plts,description,etc.)
          #5. For each record in the filtered data, make a row

          offload_numbers.each do |this_offload_number|
            data = bl_data.select{|row| row["offload_id"]==this_offload_number} #Filter the data to include only data with that offload number
            header = data[0] #Select the first row of that filtered data as the header

            #make a lambda literal to return only ex_vessel if ex_voyage is null, otherwise return both
            ex_vessel_voyage = ->(header) do
              if header["ex_voyage"]
                header["ex_vessel"]<<" "<<header["ex_voyage"]
              else
                header["ex_vessel"]
              end
            end

            #Make a header row containing that header data
            add_row ["OFFLOAD:",header["routing_number"],"Ex:",ex_vessel_voyage.call(header),nil,nil,"CTI Vessel/Voyage:",nil,header["vessel_name"]+" "+header["voyage_no"],nil],
                    [right_small,bold_and_centered,right,bold_and_centered,nil,nil,right,nil,bold_and_centered,nil],
                    [3..5,6..7,8..9]

            add_row ["QTY","UNITS","PLTS","DESCRIPTION",nil,nil,nil,"PLT WT","NET WT","GROSS WT"],
                    bold_and_centered,
                    [3..5]

            #make a row for each record in data
            data.each do |row|
              #make lambda literal to return only product if size is null, otherwise return both
              product_description = ->(row){row["product_size"] ? row["product"]<<" "<<row["product_size"] : row["product"]}
              add_row [row["qty"],row["units"],row["pallet_count"],product_description.call(row),nil,nil,nil,"=45*"+cell(current_row,2),"="+cell(current_row,9)+"-"+cell(current_row,7),row["weight"]],
                      centered,
                      [3..5]
            end
          end

          #record the row number of the last row of summary data, so that we can summarize the results below
          last_row_of_bl_summary = current_row-1

          #define a lambda literal that will return a string containing the Excel formula to sum up the bl column of our choice:
          summary = ->(column_number){"=SUM("+cell(first_row_of_bl_summary,column_number)+":"+cell(last_row_of_bl_summary,column_number)+")"}

          #make a summary row:
          add_row [summary.call(0),nil,summary.call(2),nil,nil,nil,nil,summary.call(7),summary.call(8),summary.call(9)],
                  summary_row,
                  [] #don't merge any



          #make the columns on the sheet the approriate width
          sheet.column_widths 8.43,8.43,8.43,9.57,8.43,8.43,9.43,8.43,8.43,13.14
        end



        ##################################################################################################################################################
        ##################################################################################################################################################
        ##################################################################################################################################################
        ##################################################################################################################################################


        #Make a tally sheet for each offload number present in the carrier car.
        tally_headers = get_carrier_tally_header(args[:bl_number])

        #In other words, make an entire tally sheet for each record in tally_headers.
        tally_headers.each do |tally_header|

          #Make the sheet
          wb.add_worksheet(:name => "Tally for Offload ##{tally_header["offload_id"]}-#{tally_header["extension_id"]}") do |sheet|
            #Get the detail info by referencing the id of the header:
            tally_data = get_carrier_tally_detail(tally_header["id"])
            tally_detail = tally_data[:data] #The bulk of the data on the tally sheet. Contains records of each pallet tallied.
            tally_summary = tally_data[:product_summary] #summary data, for the bottom of the tally sheet

            #cetain vars used in the sheet that draw from the detail or header:
            number_of_dunnage_pallets = tally_header["dunnage_plt_qty"]


            current_row = 0 #initialize the counter

            # We're going to have different formatting styles for different cells.
            bold_and_centered = sheet.styles.add_style(:b => true,:alignment =>{:horizontal => :center,:shrink_to_fit => true}) #bold and centered
            bold = sheet.styles.add_style(:b => true) #just bold
            centered = sheet.styles.add_style(:alignment =>{:horizontal => :center,:shrink_to_fit => true})
            right = sheet.styles.add_style(:alignment =>{:horizontal => :right,:shrink_to_fit => true})
            summary_row = sheet.styles.add_style(:b => true, :alignment =>{:horizontal => :center,:shrink_to_fit => true},:border => {:outline => true,:style => :thick,:color => "000000",:edges => [:top]})
            header_row = sheet.styles.add_style(:b => true, :sz => 10, :alignment => {:horizontal => :center,:shrink_to_fit => true})
            detail_cells = sheet.styles.add_style(:alignment =>{:horizontal => :center,:shrink_to_fit => true},:sz => 10)

            define_singleton_method("add_row") do |values,styles,merge_ranges| #making each row, takes three arguments
                                                                               #add the content to the cells from values array and set the styles for each cell from styles array
              sheet.add_row values,:style => styles
              #merge the ranges
              merge_ranges.each do |range|
                sheet.merge_cells(sheet.rows[current_row].cells[range])
              end
              current_row+=1
            end

            #make the header rows

            #title row
            add_row ["Tally Sheet",nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                    bold_and_centered,
                    [0..11]

            add_row [],[],[]

            #waybill and tally number
            add_row ["Waybill Number:",nil,nil,tally_header["waybill_id"],nil,nil,"Tally #:",nil,nil,tally_header["id"],nil,nil],
                    [right,nil,nil,bold_and_centered,nil,nil,right,nil,nil,bold_and_centered,nil,nil],
                    [0..2,3..5,6..8,9..11]

            #offload number and wireless number
            add_row ["Offload Number:",nil,nil,tally_header["offload_id"]<<"-"<<tally_header["extension_id"],nil,nil,"Wireless #:",nil,nil,tally_header["ref_tally_id"],nil,nil],
                    [right,nil,nil,bold_and_centered,nil,nil,right,nil,nil,bold_and_centered,nil,nil],
                    [0..2,3..5,6..8,9..11]

            #ex vessel and date, with slightly different formatting than the other cells
            add_row ["Ex Vessel:",nil,nil,tally_header["ex_vessel"],nil,nil,nil,nil,"Date:",tally_header["xaction_date"],nil,nil],
                    [right,nil,nil,bold_and_centered,nil,nil,nil,nil,right,bold_and_centered,nil,nil],
                    [0..2,3..7,9..11]

            3.times {|x| add_row [],[],[]}

            add_row ["LOAD","CASE CT","UNITS","TEMP","WEIGHT","TYPE","P. CODE","PRODUCT",nil,"PLTS","HS","PLT#"],
                    bold_and_centered, #all bold and centered
                    [7..8]    #no merged cells


            first_row_of_tally = current_row
            ##################################################################
            #make a row for each record in the tally detail array
            tally_detail.each do |row|
              add_row [row["line_no"],row["qty"],row["units"],row["product_temp"],row["weight"],row["weight_type"],row["prod_rate_item_id"],row["product_description"]<<"-"<<row["product_size"],nil,row["pallet_yn"],row["hand_stow_yn"],row["pallet_id"]],
                      centered,
                      [7..8]
            end
            ##################################################################
            last_row_of_tally = current_row-1

            #define a lambda literal to accept a column number as an argument and return the sum of that column of the tally
            sum = ->(col){"=SUM(#{cell(first_row_of_tally,col)}:#{cell(last_row_of_tally,col)})"}

            #make the summary row:
            #  count, sum, nil, nil, sum, nil, nil, nil,nil,sum,sum,nil
            add_row ["=COUNT(#{cell(first_row_of_tally,0)}:#{cell(last_row_of_tally,0)})",sum.call(1),nil,nil,sum.call(4),nil,nil,nil,nil,sum.call(9),sum.call(10),nil],
                    summary_row,
                    [7..8]

            #report temperature min and maxes, but getting MIN() and MAX() of the temperature column (column 3)
            add_row ["TEMP MIN:",nil,"=MIN(#{cell(first_row_of_tally,3)}:#{cell(last_row_of_tally,3)})"],
                    [right,nil,bold_and_centered],
                    [0..1]

            add_row ["TEMP MAX:",nil,"=MAX(#{cell(first_row_of_tally,3)}:#{cell(last_row_of_tally,3)})"],
                    [right,nil,bold_and_centered],
                    [0..1]


            #two blank rows
            add_row [],[],[]
            add_row [],[],[]

            #summarize the tally, grouping by product
            add_row ["SUMMARY TOTALS:",nil,nil], bold_and_centered, [0..2]


            add_row [nil,nil,nil,"PRODUCT",nil,nil,nil,"QTY","UNIT","WEIGHT",nil,"WT TYPE"],
                    [nil,nil,nil,bold_and_centered,nil,nil,nil,bold_and_centered,bold_and_centered,bold_and_centered,nil,bold_and_centered],
                    [3..6,9..10]

            tally_summary.each do |row|
              # make a row for each
              add_row [nil,nil,nil,row[:description],nil,nil,nil,row[:qty],row[:units],row[:weight],nil,row[:weight_type]],
                      [nil,nil,nil,centered,nil,nil,nil,centered,centered,centered,nil,centered],
                      [3..6,9..10]
            end









            sheet.column_widths 4.86,7.14,5.86,5.57,7.29,4.43,7.86,11.07,11.07,5.14,2.86,7.14
          end #end of worksheet
        end #end of loop through header records
      end #end of workbook
      p.serialize "#{args[:path].to_s}/Carrier_BL_#{args[:bl_number].to_s}.xlsx" #save the file
    end
  end

end
