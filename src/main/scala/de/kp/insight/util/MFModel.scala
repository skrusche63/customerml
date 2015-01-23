package de.kp.insight.util
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Shopify-Insight project
* (https://github.com/skrusche63/shopify-insight).
* 
* Shopify-Insight is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Shopify-Insight is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Shopify-Insight. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.jblas.DoubleMatrix

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.linalg.Vectors

/**
 * MFModel is a copy of Apache Spark's MatrixFactorizationModel that is still [private] 
 * for the version used here. As we need to persist the results of collaborative filtering 
 * and use the results elsewhere, we had to duplicate this class.
 */
class MFModel (
    val rank:Int,
    val rmse:Double,
    val userFeatures:RDD[(Int,Array[Double])],
    val productFeatures:RDD[(Int,Array[Double])]
  ) extends Serializable {

  /**
   * This method is added to the MFModel and differs from Apache Spark's 
   * implementation of the MatrixFactorizationModel; it has been introduced
   * to support item-item similarity based on their latent feature vectors
   * 
   * THIS METHOD DOES NOT WORK IN A DISTRIBUTED MANNER
   * 
   */
  def computeProductSimilarities:DoubleMatrix = {
    
    val dataset = productFeatures.sortBy(_._1).map(_._2).collect
    
    val dim = dataset.size
    val matrix = DoubleMatrix.zeros(dim,dim)
    /*
     * Compute the cosine similarity of the product features 
     * vectors (rows) and ignore diagonal values (=0), due to
     * the fact, that this matrix will be used to determine
     * most similar other products
     */
    (0 until dim).foreach(i => {
      (0 until dim).foreach(j => {
        if (i != j) matrix.put(i,j,CosineSimilarity.compute(dataset(i),dataset(j)))
      })
    })
    
    matrix
    
  }
   
  /**
   * This method is added to the MFModel and differs from Apache Spark's 
   * implementation of the MatrixFactorizationModel; it has been introduced
   * to support user-user similarity based on their latent feature vectors
   * 
   * THIS METHOD DOES NOT WORK IN A DISTRIBUTED MANNER
   * 
   */
  def computeUserSimilarities:DoubleMatrix = {
     
    val dataset = userFeatures.sortBy(_._1).map(_._2).collect
    
    val dim = dataset.size
    val matrix = DoubleMatrix.zeros(dim,dim)
    /*
     * Compute the cosine similarity of the user features 
     * vectors (rows) and ignore diagonal values (=0), due to
     * the fact, that this matrix will be used to determine
     * most similar other users
     */
    (0 until dim).foreach(i => {
      (0 until dim).foreach(j => {
        if (i != j) matrix.put(i,j,CosineSimilarity.compute(dataset(i),dataset(j)))
      })
    })
    
    matrix
    
  }
 
  def predict(user: Int, product: Int): Double = {

    val userVector = new DoubleMatrix(userFeatures.lookup(user).head)
    val productVector = new DoubleMatrix(productFeatures.lookup(product).head)

    userVector.dot(productVector)
  
  }

  def predict(usersProducts:RDD[(Int,Int)]):RDD[Rating] = {
   
    val users = userFeatures.join(usersProducts).map{case (user,(uFeatures,product)) => (product,(user,uFeatures))}    
    users.join(productFeatures).map{case (product, ((user, uFeatures), pFeatures)) => {
    
      val userVector = new DoubleMatrix(uFeatures)
      val productVector = new DoubleMatrix(pFeatures)
      
      Rating(user, product, userVector.dot(productVector))
    
    }}
  
  }

  def recommendProducts(user:Int,num:Int):Array[Rating] =
    recommend(userFeatures.lookup(user).head, productFeatures, num).map(t => Rating(user, t._1, t._2))

  def recommendUsers(product: Int, num: Int): Array[Rating] =
    recommend(productFeatures.lookup(product).head, userFeatures, num).map(t => Rating(t._1, product, t._2))

  private def recommend(recommendToFeatures: Array[Double],recommendableFeatures: RDD[(Int, Array[Double])],num: Int): Array[(Int, Double)] = {
    
    val recommendToVector = new DoubleMatrix(recommendToFeatures)
    val scored = recommendableFeatures.map { case (id,features) =>
      (id, recommendToVector.dot(new DoubleMatrix(features)))
    }
    
    scored.top(num)(Ordering.by(_._2))
  
  }  
}
