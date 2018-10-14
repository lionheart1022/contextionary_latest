DATABASE = {
  'host': 'localhost',
  'dbname': 'contextionary',
  'user': 'postgres',
  'password': 'postgres'
}

PARSE = {
  'rootDirectory' : "Test",
  'phraseMaxLength': 5,
  'minPhraseCount' : 2,
  'percentileThreshold': 0.2,
  'crossPresenceThreshold': 0.50,
  'phraseWeightMethod' : "freq",
  'longPhraseLength': 2,
  'similarityThreshold': 2,
  'contextWeightRankThreshold': 2
}

# PARSE = {
#   'rootDirectory' : "Test",
#   'phraseMaxLength': 3,
#   'minPhraseCount' : 1,
#   'percentileThreshold': 0.2,
#   'crossPresenceThreshold': 0.50,
#   'phraseWeightMethod' : "dist",
#   'longPhraseLength': 2,
#   'similarityThreshold': 2,
#   'contextWeightRankThreshold': 2
# }