package com.realcurrents.controllers

import com.realcurrents.WebClientDownloader
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.delay
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.scheduler.Schedulers
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.streams.asStream

@RestController
@RequestMapping(produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
public class WebClientSampleController {

    @Autowired
    private lateinit var downloader: WebClientDownloader

    @GetMapping("/downThemAll")
    fun getAllFiles (): ResponseEntity<Any> {
        val quickResponse: HashMap<String, Any> = HashMap<String, Any>()
        quickResponse.set("Status", "Downloading all files at "+ downloader.base)

        return ResponseEntity.ok(quickResponse)
    }

    @GetMapping("/downThemSmall")
    fun getSmallFiles (): ResponseEntity<Any> {
        val quickResponse: HashMap<String, Any> = HashMap<String, Any>()
        quickResponse.set("Status", "Downloading all files at "+ downloader.base)

        val bigD = AtomicLong(0)

        val downloads = AtomicInteger(0)

        val downloadCount = 8

        val fileList = listOf(
            "small.01.zip",
            "small.02.zip",
            "small.03.zip",
            "small.04.zip",
            "small.05.zip",
            "small.06.zip",
            "small.07.zip",
            "small.08.zip"
        )

        var setList: MutableList<Flux<String>> = mutableListOf()

        fileList
            .forEach{fileName ->
                println("Getting "+ fileName)
                setList.add(downloader.retrieveFile(fileName))
                downloads.addAndGet(1)
            }

        while (setList.size > 0) async {
            val name: Array<String> = Array<String>(1, { "" })
            val fileStream = setList.last()
            val disposable: Disposable = fileStream.subscribe{ content ->
                println(content)

                println("${downloads.get()} resources loaded")

                if (downloads.get() >= downloadCount) {
                    println("Finished after loading ${downloads.get()} of ${downloadCount} resources")
                    println("DOWNLOADED ${bigD.get()} BYTES!")

                } else if ((downloads.get() % 8).toInt() == 0) {
                    println("Finished set ${(downloads.get()/8)} of 1")
                }

                setList.remove(fileStream)

                name[0] = content
            }

            println("Close WebClient connection and dispose of ${name[0]}")

            /* After ... seconds, close the WebClient connection */
            delay((Math.random() * 999).toLong() + 30000, TimeUnit.MILLISECONDS)
            disposable.dispose()

            async { System.gc() }
        }

        return ResponseEntity.ok(quickResponse)
    }

    @GetMapping("/{fileName}")
    fun getFile (@PathVariable fileName: String): ResponseEntity<Any> {
        val quickResponse = HashMap<String, Any>()
        val file = downloader.retrieveFile(fileName)
            .flatMap({ v -> Mono.just<String>(v).subscribeOn(Schedulers.parallel()) }, 2)

        quickResponse.set("Status", "Downloading "+ fileName)
        return ResponseEntity.ok(quickResponse)
    }
}
