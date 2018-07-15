
import org.apache.spark.sql.DataFrame


import scala.util.control.Breaks._

import org.apache.spark.sql._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class BivariateAnalysis {
  
val spark = SparkSession.builder().getOrCreate();
import spark.implicits._;

def poz(z : Double): Double = {
        var y , x , w=0.0;
        
        if (z == 0.0) {
            x = 0.0;
        } else {
            y = 0.5 * Math.abs(z);
            if (y > (6 * 0.5)) {
                x = 1.0;
            } else if (y < 1.0) {
                w = y * y;
                x = ((((((((0.000124818987 * w
                         - 0.001075204047) * w + 0.005198775019) * w
                         - 0.019198292004) * w + 0.059054035642) * w
                         - 0.151968751364) * w + 0.319152932694) * w
                         - 0.531923007300) * w + 0.797884560593) * y * 2.0;
            } else {
                y -= 2.0;
                x = (((((((((((((-0.000045255659 * y
                               + 0.000152529290) * y - 0.000019538132) * y
                               - 0.000676904986) * y + 0.001390604284) * y
                               - 0.000794620820) * y - 0.002034254874) * y
                               + 0.006549791214) * y - 0.010557625006) * y
                               + 0.011630447319) * y - 0.009279453341) * y
                               + 0.005353579108) * y - 0.002141268741) * y
                               + 0.000535310849) * y + 0.999936657524;
            }
        }
        if(z < 0.0) 
            return ((x + 1.0) * 0.5) 
            
        return ((1.0 - x) * 0.5);
    }
/*def pnorm(z : Double) :Double = {
  var x = Math.sqrt(1/2)*Math.abs(z);
  
  // compute erfc(x) using an approximation formula (max rel error = 1.2e-7) 
  // see https://en.wikipedia.org/wiki/Error_function#Numerical_approximation
  var t = 1.0/(1+0.5*x);
  var t2 = t*t;
  var t3 = t2*t; 
  var t4 = t2*t2;
  var t5 = t2*t3; 
  var t6 = t3*t3; 
  var t7 = t3*t4;
  var t8 = t4*t4;
  var t9 = t4*t5;
  var tau = -x*x - 1.26551223 + 1.00002368*t + 0.37409196*t2 + 0.09678418*t3 - 0.18628806*t4 + 0.27886807*t5 - 1.13520398*t6 + 1.48851587*t7 - 0.82215223*t8 + 0.17087277*t9;

  var p = 0.5*t*Math.exp(tau);

  if (z < 0) {
     p = 1-p;
     
  }
  
 
  return p;
}*/
    
def zTest(df : DataFrame, continuousCol : String, groupByCol : String) : String ={
    val groups = df.groupBy(col(groupByCol)).agg(stddev_pop(col(continuousCol)).as("SD"),count(col(continuousCol)).as("Count"),avg(col(continuousCol)).as("Average"))
    val groupCount = groups.select(groupByCol).count
    if(groupCount!=2)
    {
        return "Total Number of Categories are not equal to 2. Use ANOVA for the data."
    }
    
    val avg1 = groups.head.getDouble(3)
    val cov1 = Math.pow(groups.head.getDouble(1),2)
    val count1 = groups.head.getLong(2)
    
    val row2 = groups.rdd.zipWithIndex.filter(_._2==1).map(_._1).first()
    val avg2 = row2.getDouble(3)
    val cov2 = Math.pow(row2.getDouble(1),2)
    val count2 = row2.getLong(2)
    if(count1 <30 || count2<30)
        {
            return "Frequency of one category is less than 30. Please use T-Test for the data."
        }
    
    val rslt = (avg1-avg2)/Math.sqrt((cov1/count1)+(cov2/count2)) 
    return "ZTest Value for "+groupByCol+" & "+continuousCol+" = "+rslt + "\nRight Tail Probability Value for "+groupByCol+" & "+continuousCol+" = "+(poz(rslt))
    
}

def gamnln(n : Integer) : Double = {
  // Tabulated values of ln(Gamma(n/2)) for n<201
  var lg = Array(0.5723649429247001, 0, -0.1207822376352452, 0, 0.2846828704729192, 0.6931471805599453, 1.200973602347074, 1.791759469228055, 2.453736570842442, 3.178053830347946, 3.957813967618717, 4.787491742782046, 5.662562059857142, 6.579251212010101, 7.534364236758733, 8.525161361065415, 9.549267257300997, 10.60460290274525, 11.68933342079727, 12.80182748008147, 13.94062521940376, 15.10441257307552, 16.29200047656724, 17.50230784587389, 18.73434751193645, 19.98721449566188, 21.2600761562447, 22.55216385312342, 23.86276584168909, 25.19122118273868, 26.53691449111561, 27.89927138384089, 29.27775451504082, 30.67186010608068, 32.08111489594736, 33.50507345013689, 34.94331577687682, 36.39544520803305, 37.86108650896109, 39.3398841871995, 40.8315009745308, 42.33561646075349, 43.85192586067515, 45.3801388984769, 46.91997879580877, 48.47118135183522, 50.03349410501914, 51.60667556776437, 53.19049452616927, 54.78472939811231, 56.38916764371993, 58.00360522298051, 59.62784609588432, 61.26170176100199, 62.9049908288765, 64.55753862700632, 66.21917683354901, 67.88974313718154, 69.56908092082364, 71.257038967168, 72.9534711841694, 74.65823634883016, 76.37119786778275, 78.09222355331531, 79.82118541361436, 81.55795945611503, 83.30242550295004, 85.05446701758153, 86.81397094178108, 88.58082754219767, 90.35493026581838, 92.13617560368709, 93.92446296229978, 95.71969454214322, 97.52177522288821, 99.33061245478741, 101.1461161558646, 102.9681986145138, 104.7967743971583, 106.6317602606435, 108.4730750690654, 110.3206397147574, 112.1743770431779, 114.0342117814617, 115.9000704704145, 117.7718813997451, 119.6495745463449, 121.5330815154387, 123.4223354844396, 125.3172711493569, 127.2178246736118, 129.1239336391272, 131.0355369995686, 132.9525750356163, 134.8749893121619, 136.8027226373264, 138.7357190232026, 140.6739236482343, 142.617282821146, 144.5657439463449, 146.5192554907206, 148.477766951773, 150.4412288270019, 152.4095925844974, 154.3828106346716, 156.3608363030788, 158.3436238042692, 160.3311282166309, 162.3233054581712, 164.3201122631952, 166.3215061598404, 168.3274454484277, 170.3378891805928, 172.3527971391628, 174.3721298187452, 176.3958484069973, 178.4239147665485, 180.4562914175438, 182.4929415207863, 184.5338288614495, 186.5789178333379, 188.6281734236716, 190.6815611983747, 192.7390472878449, 194.8005983731871, 196.86618167289, 198.9357649299295, 201.0093163992815, 203.0868048358281, 205.1681994826412, 207.2534700596299, 209.3425867525368, 211.435520202271, 213.5322414945632, 215.6327221499328, 217.7369341139542, 219.8448497478113, 221.9564418191303, 224.0716834930795, 226.1905483237276, 228.3130102456502, 230.4390435657769, 232.5686229554685, 234.7017234428182, 236.8383204051684, 238.9783895618343, 241.1219069670290, 243.2688490029827, 245.4191923732478, 247.5729140961868, 249.7299914986334, 251.8904022097232, 254.0541241548883, 256.2211355500095, 258.3914148957209, 260.5649409718632, 262.7416928320802, 264.9216497985528, 267.1047914568685, 269.2910976510198, 271.4805484785288, 273.6731242856937, 275.8688056629533, 278.0675734403662, 280.2694086832001, 282.4742926876305, 284.6822069765408, 286.893133295427, 289.1070536083976, 291.3239500942703, 293.5438051427607, 295.7666013507606, 297.9923215187034, 300.2209486470141, 302.4524659326413, 304.6868567656687, 306.9241047260048, 309.1641935801469, 311.4071072780187, 313.652829949879, 315.9013459032995, 318.1526396202093, 320.4066957540055, 322.6634991267262, 324.9230347262869, 327.1852877037753, 329.4502433708053, 331.7178871969285, 333.9882048070999, 336.2611819791985, 338.5368046415996, 340.815058870799, 343.0959308890863, 345.3794070622669, 347.6654738974312, 349.9541180407703, 352.2453262754350, 354.5390855194408, 356.835382823613, 359.1342053695754);
  
  
  if(n<201)
    return lg(n-1);
  

  // For n>200, use the approx. formula given by numerical recipe
  var coef = Array(76.18009172947146, -86.50532032941677, 24.01409824083091, -1.231739572450155, 1.208650973866179e-3, -5.395239384953e-6);
  var stp = 2.5066282746310005;
  var x = 0.5*n;
  var y = x;
  var tmp = x + 5.5;
  tmp = (x+0.5)*Math.log(tmp) - tmp;
  var ser = 1.000000000190015; 
  for (i<- 1 to 5) {
     y = y + 1;
     ser = ser + coef(i)/y;
  }
  var rslt = tmp + Math.log(stp*ser/x);
  return rslt;
}

def betacf(a : Double,b : Double ,x : Double) : Double = {
  var qab = a+b;
  var qap = a+1.0;
  var qam = a-1.0;
  var c = 1.0;
  var d = 1 - qab*x/qap;
  var fpmin = 1.0e-300;
  var eps = 1.0e-6;
  if (Math.abs(d) < fpmin) { d = fpmin;}
  d = 1.0/d;
  var h=d;
  breakable { for (m<-1 to 1000) {
     var m2 = 2*m;
     var aa = m*(b-m)*x/((qam+m2)*(a+m2));
     d = 1+aa*d;
     if (Math.abs(d) < fpmin) { d = fpmin;}
     c = 1+aa/c;
     if (Math.abs(c) < fpmin) { c = fpmin;}
     d = 1.0/d;
     h = h*d*c;
     aa = -(a+m)*(qab+m)*x/((a+m2)*(qap+m2));
     d = 1+aa*d;
     if (Math.abs(d) < fpmin) { d = fpmin;}
     c = 1+aa/c;
     if (Math.abs(c) < fpmin) { c = fpmin;}
     d = 1.0/d;
     var del = d*c;
     h = h*del;
     if (Math.abs(del-1.0) < eps) { break;}
  } }
  return h;
}



def betai(n : Integer,m : Integer ,x : Double ) : Double ={
  var bt=0.0;
  var a = 0.5*n;
  var b = 0.5*m;
  if (x==0 || x==1) {
    bt = 0.0;
  } else {
    bt = Math.exp(gamnln(m+n)-gamnln(n)-gamnln(m) + a*Math.log(x) + b*Math.log(1-x) );
  }
  var beti=0.0;
  if (x < (a+1.0)/(a+b+2)) {
    // use continued fraction directly
    beti = bt*betacf(a,b,x)/a;
  } else {
    // use continued fraction after making the symmetry transformation
    beti = 1.0 - bt*betacf(b,a,1-x)/b;
  }
  return beti;
}


def pf(F : Double ,df1 : Integer ,df2: Integer) : Double = {
   
     var x = df2.toDouble/(df1*F + df2);
     return betai(df2,df1,x);
  
}

/*def anova(df : DataFrame, continuousCol : String, groupByCol : String) : String ={
    val groups = df.groupBy(col(groupByCol)).agg(avg(col(continuousCol)).as("Average"))
    val valueCount = df.select(continuousCol).count
    val groupCount = groups.select(groupByCol).count.toInt
    
    
    
    val mean = df.select(avg(col(continuousCol))).head.getDouble(0)
    var SSE = 0.0
    var SST = 0.0
    for(i<-0 to (groupCount-1)){
        val curGroup = groups.rdd.zipWithIndex.filter(_._2==i).map(_._1).first()
        val curGroupName = curGroup.get(0)
        val curGroupAvg = curGroup.getDouble(1)
        val df1  = df.where(col(groupByCol)===curGroupName).select((col(continuousCol).minus(curGroupAvg)).as("SSE"))
        SSE = SSE + df1.agg(sum(col("SSE")*col("SSE"))).head.getDouble(0)
        SST = SST + Math.pow(mean-curGroupAvg,2)
    }
    
    val SSTT = SSE + SST
    val DFT = groupCount-1
    val DFE = valueCount - groupCount
    val MST = SST/DFT
    val MSE = SSE/DFE
    val fStat = MST/MSE
    
    val etaSq = SST/SSTT
    val omegaSq = (SST - ((groupCount - 1) * MSE))/(SSTT + MSE)
    val pVal = pf(fStat,DFE.toInt,DFT.toInt)
    return "SSE = "+SSE.toString + " \t SST = "+ SST.toString + "\n"+
                    "DFE = "+ DFE.toString + "\t DFT = "+ DFT.toString + "\n"+
                    "MSE = "+ MSE.toString + "\t MST = "+ MST.toString + "\n"+
                    "F Statistics = " + fStat.toString + "\n"+"EtaSq = "+ etaSq.toString + "\t OmegaSq = "+ omegaSq.toString +
                    "\nP Value = " + pVal
    
}*/
def anova(df : DataFrame, continuousCol : String, groupByCol : String) : String ={
    val groups = df.groupBy(col(groupByCol)).agg(avg(col(continuousCol)).as("Average"))
    val valueCount = df.select(continuousCol).count
    val groupCount = groups.select(groupByCol).count.toInt
    
    
    
    val mean = df.select(avg(col(continuousCol))).head.getDouble(0)
    var BSS = 0.0
    var WSS = 0.0
    var TSS = 0.0
    
    var tmpDF = df.select(col(continuousCol).minus(mean).as("diff"))
    
    TSS = tmpDF.agg(sum(col("diff")*col("diff"))).head.getDouble(0);
    
    for(i<-0 to (groupCount-1)){
        val curGroup = groups.rdd.zipWithIndex.filter(_._2==i).map(_._1).first()
        val curGroupName = curGroup.get(0)
        val curGroupAvg = df.where(col(groupByCol)===curGroupName).agg(avg(col(continuousCol))).head.getDouble(0)
        val curGroupCount = df.where(col(groupByCol)===curGroupName).count
        val df1  = df.where(col(groupByCol)===curGroupName).select((col(continuousCol).minus(curGroupAvg)).as("SSW"))
        BSS = BSS + curGroupCount*Math.pow((mean-curGroupAvg),2)
        WSS = WSS + df1.agg(sum(col("SSW")*col("SSW"))).head.getDouble(0)
        
    }
    
    
    val DFT = valueCount-1
    val DFB = groupCount -1
    val DFW = DFT-DFB
    
    val MSB = BSS/DFB
    val MSW = WSS/DFW
    
    
    val fStat = MSB/MSW
    
  //  val etaSq = SST/SSTT
    //val omegaSq = (SST - ((groupCount - 1) * MSE))/(SSTT + MSE)
    val pVal = pf(fStat,DFT.toInt,DFB.toInt)
    return "TSS = "+TSS.toString + " \t WSS = "+ WSS.toString +"\t BSS = " + BSS.toString +"\n"+
                    "DFB = "+ DFB.toString + "\t DFT = "+ DFT.toString + "\n"+
                    "MSB = "+ MSB.toString + "\t MSW = "+ MSW.toString + "\n"+
                    "F Statistics = " + fStat.toString + //"\n"+"EtaSq = "+ etaSq.toString + "\t OmegaSq = "+ omegaSq.toString +
                    "\nP Value = " + pVal
    
}


def pot(t: Double,n : Integer) :Double = {
   var x = 1.0*n/(t*t+n);
   //var x = 1.0/(n*t+1)
   var p = betai(n,1,x);
   
     if (t > 0) {
       p = 0.5*p;
     } else {
       p = 1-0.5*p;
     }
   
   return p;
}


def tTest(df : DataFrame, continuousCol : String, groupByCol : String) : String ={
    val groups = df.groupBy(col(groupByCol)).agg(stddev_pop(col(continuousCol)).as("SD"),count(col(continuousCol)).as("Count"),avg(col(continuousCol)).as("Average"))
    val groupCount = groups.select(groupByCol).count
    if(groupCount!=2)
    {
        return "Total Number of Categories are not equal to 2. Use ANOVA for the data."
    }
    
    val avg1 = groups.head.getDouble(3)
    val cov1 = Math.pow(groups.head.getDouble(1),2)
    val count1 = groups.head.getLong(2)
    
    val row2 = groups.rdd.zipWithIndex.filter(_._2==1).map(_._1).first()
    val avg2 = row2.getDouble(3)
    val cov2 = Math.pow(row2.getDouble(1),2)
    val count2 = row2.getLong(2)
    
    if(count1 >50 && count2>50)
    {
        return "Frequency of both categories is more than 50. Please use Z Test for the data."
    }
    val s2 = (((count1 - 1)*cov1) + ((count2 - 1)*cov2))/(count1+count2-2)
    
    val dof = (count1+count2-2);
    
    val rslt = (avg1-avg2)/Math.sqrt(((s2/count1)+(s2/count2)))
    return "T-Test Value for "+groupByCol+" & "+continuousCol+" = "+rslt + "\nProbability Value for "+groupByCol+" & "+continuousCol+" = "+(pot(rslt,dof.toInt))
    
}




}