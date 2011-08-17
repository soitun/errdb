require 'errdb'

r = Errdb.new("localhost", 7272)

r.fetch('key', 0, 9999999999)

