class Sailing < Sequel::Model(:sailing_date)
  extends ActiveModel::Naming

  set_primary_key [:sailing_id]


end