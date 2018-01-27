package com.realcurrents

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class WebClientSampleApplication

fun main(args: Array<String>) {
    runApplication<WebClientSampleApplication>(*args)
}
