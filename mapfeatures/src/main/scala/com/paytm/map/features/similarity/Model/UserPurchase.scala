package com.paytm.map.features.similarity.Model

case class UserPurchase(createdAt: String, productId: Long)
case class CustomerScore(customer_id: Long, score: Double)
case class CooccurenceCount(ida: Long, idb: Long, occurence_count: Long, customer_count: Long)