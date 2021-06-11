//Loading the data

val df = sc.textFile("heart.csv");

val header = df.first();

val data = df.filter(row => row!=header);

val sep = data.map{l=>
     val s0 = l.split(",")
     val (age,sex,cp,trestbps,chol,fbs,restecg,thalach,exang,oldpeak,slope,ca,thal,target)=(s0(0).toInt,s0(1).toInt,s0(2).toInt,s0(3).toInt,s0(4).toInt,s0(5).toInt,s0(6).toInt,s0(7).toInt,s0(8).toInt,s0(9).toFloat,s0(10).toInt,s0(11).toInt,s0(12).toInt,s0(13).toInt)
     (age,sex,cp,trestbps,chol,fbs,restecg,thalach,exang,oldpeak,slope,ca,thal,target)}

sep.toDF("age","sex","cp","trestbps","chol","fbs","restecg","thalach","exang","oldpeak","slope","ca","thal","target").show(false);

//Analysis
//Age vs Target
 val age_target = sep.sortBy(_._1).map{case(age,sex,cp,trestbps,chol,fbs,restecg,thalach,exang,oldpeak,slope,ca,thal,target)=>(age,(target,1))};
 val age_target_sum = age_target.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2));
 val age_target_avg = age_target_sum.map{case(a,b)=>(a,b._1/b._2.toFloat)}.sortBy(_._1);
 age_target_avg.toDF("Age","Affected Percent").show(false);

 //Gender vs Target
 val sex_target = sep.sortBy(_._1).map{case(age,sex,cp,trestbps,chol,fbs,restecg,thalach,exang,oldpeak,slope,ca,thal,target)=>(sex,(target,1))}
 val sex_target_sum = sex_target.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2));
 val sex_target_avg = sex_target_sum.map{case(a,b)=>(a,b._1/b._2.toFloat)}.sortBy(_._1);
 sex_target_avg.toDF("Sex","Affected Percent").show(false);

 //thal vs Target
 val thal_target = sep.sortBy(_._1).map{case(age,sex,cp,trestbps,chol,fbs,restecg,thalach,exang,oldpeak,slope,ca,thal,target)=>(thal,(target,1))};
 val thal_target_sum = thal_target.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2));
 val thal_target_avg = thal_target_sum.map{case(a,b)=>(a,b._1/b._2.toFloat)}.sortBy(_._1);
 thal_target_avg.toDF("Thal","Affected Percent").show(false);

//chol vs Target
 val chol_target = sep.sortBy(_._1).map{case(age,sex,cp,trestbps,chol,fbs,restecg,thalach,exang,oldpeak,slope,ca,thal,target)=>(chol,(target,1))};
 val chol_target_sum = chol_target.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2));
 val chol_target_avg = chol_target_sum.map{case(a,b)=>(a,b._1/b._2.toFloat)}.sortBy(_._1);
 chol_target_avg.toDF("Cholestral","Affected Percent").show(false);

 //(Age,Sex) vs Target
 val ageSex_target = sep.sortBy(_._1).map(l=>((l._1,l._2),(l._14,1)))
 val ageSex_target_reduced = ageSex_target.reduceByKey((x,y)=>((x._1+y._1),(x._2+y._2))).sortBy(_._1)
 val ageSex_target_avg = ageSex_target_reduced.map{case(a,b)=>(a,b,((b._1).toFloat/(b._2).toFloat).toFloat)}
 ageSex_target_avg.toDF("(Age,Sex)","(nAffected,nPersons)","Affected Percent").show(false)

 //(Sex,ChestPain) vs Target
 val sexCP_target = sep.sortBy(_._1).map(l=>((l._2,l._3),(l._14,1)))
 val sexCP_target_reduced = sexCP_target.reduceByKey((x,y)=>((x._1+y._1),(x._2+y._2))).sortBy(_._1)
 val sexCP_target_avg = sexCP_target_reduced.map{case(a,b)=>(a,b,((b._1).toFloat/(b._2).toFloat).toFloat)}
 sexCP_target_avg.toDF("(Sex,ChestPain)","(nAffected,nPersons)","Affected Percent").show(false)

  //(Age,ChestPain) vs Target
 val ageCP_target = sep.sortBy(_._1).map(l=>((l._1,l._3),(l._14,1)))
 val ageCP_target_reduced = ageCP_target.reduceByKey((x,y)=>((x._1+y._1),(x._2+y._2))).sortBy(_._1)
 val ageCP_target_avg = ageCP_target_reduced.map{case(a,b)=>(a,b,((b._1).toFloat/(b._2).toFloat).toFloat)}
 ageCP_target_avg.toDF("(Age,ChestPain)","(nAffected,nPersons)","Affected Percent").show(false)
