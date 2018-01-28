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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@RestController
@RequestMapping(produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
public class WebClientSampleController {

    private val setLimit = 8;

    @Autowired
    private lateinit var downloader: WebClientDownloader

    @GetMapping("/downThemAll")
    fun getAllFiles (): ResponseEntity<Any> {
        val quickResponse: HashMap<String, Any> = HashMap<String, Any>()
        quickResponse.set("Status", "Downloading set of all files from "+ downloader.base)

        retrieveFileSet("small")
        retrieveFileSet("medium")
        retrieveFileSet("large")
        retrieveFileSet("xlarge")

        return ResponseEntity.ok(quickResponse)
    }

    @GetMapping("/downThemSmall")
    fun getSmallFiles (): ResponseEntity<Any> {
        val quickResponse: HashMap<String, Any> = HashMap<String, Any>()

        quickResponse.set("Status", "Downloading set of small files from "+ downloader.base)

        println(quickResponse.get("Status"))

        retrieveFileSet("small")

        return ResponseEntity.ok(quickResponse)
    }

    @GetMapping("/downThemMedium")
    fun getMediumFiles (): ResponseEntity<Any> {
        val quickResponse: HashMap<String, Any> = HashMap<String, Any>()

        quickResponse.set("Status", "Downloading set of medium files from "+ downloader.base)

        println(quickResponse.get("Status"))

        retrieveFileSet("medium")

        return ResponseEntity.ok(quickResponse)
    }

    @GetMapping("/downThemLarge")
    fun getLargeFiles (): ResponseEntity<Any> {
        val quickResponse: HashMap<String, Any> = HashMap<String, Any>()

        quickResponse.set("Status", "Downloading set of large files from "+ downloader.base)

        println(quickResponse.get("Status"))

        retrieveFileSet("large")

        return ResponseEntity.ok(quickResponse)
    }

    @GetMapping("/downThemExtraLarge")
    fun getExtraLargeFiles (): ResponseEntity<Any> {
        val quickResponse: HashMap<String, Any> = HashMap<String, Any>()

        quickResponse.set("Status", "Downloading set of xlarge files from "+ downloader.base)

        println(quickResponse.get("Status"))

        retrieveFileSet("xlarge")

        return ResponseEntity.ok(quickResponse)
    }

    @GetMapping("/{fileName}")
    fun getFile (@PathVariable fileName: String): ResponseEntity<Any> {
        val quickResponse = HashMap<String, Any>()
        when (fileName) {
            Size.SMALL.prefix, Size.MEDIUM.prefix, Size.LARGE.prefix, Size.XLARGE.prefix ->
                retrieveFileSet(fileName)
            else ->
                downloader.retrieveFile(fileName).subscribe{println("Getting "+ it)}
        }
        quickResponse.set("Status", "Downloading "+ fileName)
        return ResponseEntity.ok(quickResponse)
    }

    fun retrieveFileSet (setSize: String): Unit {
        val bigD = AtomicLong(0)
        val downloads = AtomicInteger(0)
        val downloadCount = 8
        val fileList = listOf(
            "${setSize}.01.zip",
            "${setSize}.02.zip",
            "${setSize}.03.zip",
            "${setSize}.04.zip",
            "${setSize}.05.zip",
            "${setSize}.06.zip",
            "${setSize}.07.zip",
            "${setSize}.08.zip"
        )

        var setList: MutableList<Flux<Array<String>>> = mutableListOf()

        fileList
            .forEach{
                println("Getting "+ it)
                setList.add(downloader.retrieveFile(it))
            }

        while (setList.size > 0) async {
            val name: Array<String> = Array<String>(1, { "" })
            val disposable: Disposable = setList.last().subscribe { content ->
                if (content[1] == "200") {
                    bigD.addAndGet(content[4].toLong())

                    println("${downloads.addAndGet(1)} resources loaded")

                    if ((downloads.get() % setLimit).toInt() == 0) {
                        println("Finished set ${(downloads.get()/ setLimit)}")
                    }
                }

                if (downloads.get() >= downloadCount) {
                    println("Finished after loaded ${downloads.get()} of ${downloadCount} resources")
                    println("DOWNLOADED ${bigD.get()} BYTES!")

                }

                name[0] = content[0]
            }

            setList.remove(setList.last())

            /* After ... seconds, close the WebClient connection */
            delay((Math.random() * 999).toLong() + setLimit *30000, TimeUnit.MILLISECONDS)

            println("Close WebClient connection and dispose of ${name[0]}")
            disposable.dispose()

            System.gc()
        }
    }
}

enum class Size (val prefix: String) {
    SMALL("small"), MEDIUM("medium"), LARGE("large"), XLARGE("xlarge");
}
