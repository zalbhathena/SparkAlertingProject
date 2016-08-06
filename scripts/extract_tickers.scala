val nasdaq = sc.textFile("data/nasdaqlisted.txt")
val other = sc.textFile("data/otherlisted.txt")

val all = nasdaq.union(other)
def split(x:String): String = {
        val splitX = x.split('|')
        var stringToReturn = ""
        if(splitX.length >= 2) {
                return splitX(0) + "|" + splitX(1)
        }
        return ""
}
val allCut = all.map(x => split(x)).filter(x => x.length > 0)
//First data set
allCut.saveAsTextFile("data/tickersToCompanyNames")
