DATA_LOC = "quantx/data"
BASE_LOG_PATH = "quantx/logs"

START_DATE = "20250217"
END_DATE = "20250217"
DATA_BUILDING_DATE = "20250217"
UNIVERSE = ["757"] # All tokens that need to be included
# UNIVERSE = ["163"] # All tokens that need to be included
# UNIVERSE = ["163", "526", "30108", "10794", "19585"]
# UNIVERSE = [1270]
# UNIVERSE = ["1270", 
# "383"]
# UNIVERSE = ["1270", 
# "383",
# "526" ,    
# "7",
# "10794",   
# "404",    
# "11439",   
# "19585",    
# "1901" ,    
# "21174"]
FILL_TYPE=["ON_OPEN"] # ON_OPEN, ON_CLOSE, ON_VWAP
# keep as zero if universe is defined
# else top num_tokens tokens will comprise the universe
NUM_TOKENS = 0


# 163      [21915, orders]
# 526      [21843, orders]
# 30108    21675
# 10794    21590
# 19585    21521


# 1270     21896
# 383      21888
# 526      21859
# 7        21767
# 10794    21754
# 404      21671
# 11439    21598
# 19585    21553
# 1901     21508
# 21174    21350
