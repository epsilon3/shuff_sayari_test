
import scala.xml.XML
import scala.collection.mutable.ListBuffer

object xmlListsFromXML {
    def isIDPresent(strID: String, lstIn: ListBuffer[(String,String,String)]) : Boolean = {
        var bIsIDPresent = false
        for (rowIn <- lstIn) {
            if(rowIn._1 == strID) {
                bIsIDPresent = true
            }
        }
        return bIsIDPresent
    }

    def listFromXML(strInFileName:String, strIDColName:String, strFirstNameColName:String, strLastNameColName:String) : ListBuffer[(String,String,String)] = {
        var lstOut = new ListBuffer[(String,String,String)]()
        var xmlIn = XML.loadFile(strInFileName)
	    for(rowIn <- xmlIn.child) {
            var strID = ""
            var strFirstName = ""
            var strLastName = ""
            for (colIn <- rowIn.child) {
                if (colIn.label == strIDColName) {
                    strID = colIn.text
                }
                else if (colIn.label == strFirstNameColName) {
                    strFirstName = colIn.text
                }
                else if (colIn.label == strLastNameColName) {
                    strLastName = colIn.text
                }
            }

            if (strID != "" && strFirstName != "" && strLastName != "") {
                if (isIDPresent(strID, lstOut) == false) {
                    lstOut += Tuple3(strID,strFirstName,strLastName)
                }
            }
        }
        return lstOut
    }
}

var lstSDN=xmlListsFromXML.listFromXML("SDN.xml","uid","firstName","lastName")
var lstConList=xmlListsFromXML.listFromXML("ConList.xml","GroupID","name1","Name6")

var lstOut = new ListBuffer[(String,String,String,String)]()
for (rowIn <- lstSDN) {
    for (rowConList <- lstConList) {
        if(rowIn._2.toLowerCase()==rowConList._2.toLowerCase() && rowIn._3.toLowerCase()==rowConList._3.toLowerCase()) {
            lstOut += Tuple4(rowIn._1,rowConList._1,rowIn._2,rowIn._3)
        }
    }
}

var dfOut = lstOut.toDF("ofac_id","uk_id","first_name","last_name")
dfOut.coalesce(1).write.option("header",true).csv("shuff_sayari_test.csv")
