# verify_mongodb.ps1
# Prints every exchange-rate document stored in MongoDB (currency_db.exchange_rates)
# plus the total count. Credentials are read from the container's own environment
# variables, so no password is written here or shown on screen.
#
# Usage (from the project root):
#   powershell -ExecutionPolicy Bypass -File scripts\verify_mongodb.ps1

$query = "print('_id                       CUR  RATE      DATE        RETRIEVED_AT'); " +
         "db.exchange_rates.find().sort({rate_date: 1, target_currency: 1}).forEach(d => " +
         "print(d._id + '  ' + d.target_currency + '  ' + String(d.exchange_rate).padEnd(8) + '  ' + d.rate_date + '  ' + d.retrieved_at)); " +
         "print('TOTAL DOCUMENTS: ' + db.exchange_rates.countDocuments())"

$query | docker exec -i currency_mongodb bash -c 'mongosh --quiet -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin currency_db'
