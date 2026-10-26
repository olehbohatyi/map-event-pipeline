package com.github.olehbohatyi.mep.domain

// A first-level administrative region (an oblast, for Ukraine)
final case class Region(
    code: String,   // ISO 3166-2, e.g. UA-46
    name: String,
    country: String, // ISO 3166-1 alpha-2
  )
