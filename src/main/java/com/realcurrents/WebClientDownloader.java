package com.realcurrents;

import org.jetbrains.annotations.NotNull;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class WebClientDownloader {
    
    private final String base;
    static int mark = 5251073;
    static int retry = 3;
    
    public WebClientDownloader (String url) {
        this.base = url;
    }
    
    public Flux<String[]> retrieveFile (String file) throws IOException {
        final AtomicInteger chunks = new AtomicInteger(1);
        final AtomicLong partBytes = new AtomicLong(0);
        final AtomicLong totalBytes = new AtomicLong(0);
        final URI url = URI.create(this.base + file);
        final String[] info = new String[5]; // { url, status, disposition (server-assigned name), type, size }
        Mono<ClientResponse> response;
        
        while (retry > 0) try {
            ClientRequest downloadRequest = ClientRequest.method(HttpMethod.GET, url)
                .header("Accept", "application/octet-stream")
                .header("TE", "chunked")
                .build();
            
            response = ExchangeFunctions.create(new ReactorClientHttpConnector())
                .filter(ExchangeFilterFunction.ofRequestProcessor(
                    req -> {
                        System.out.println(req.url());
                        info[0] = req.url().toString();
                        
                        req.headers().forEach(
                            (key, value) -> {
                                System.out.print(key + ": ");
                                System.out.println(value);
                            });
                        return Mono.just(req);
                    })
                )
                .exchange(downloadRequest);
            
            Flux<String[]> flatMapResult = response.flatMapMany((ClientResponse res) -> {
                System.out.println(res.statusCode());
                info[1] = res.statusCode().toString();
                
                HttpHeaders httpHeaders = res.headers().asHttpHeaders();
                for (Object header : httpHeaders.keySet().toArray()) {
                    String value = httpHeaders.get(header).toString()
                        .replaceAll("\\[", "")
                        .replaceAll("\\]", "");
                    switch (header.toString()) {
                        case "Content-Disposition":
                            System.out.print(header + ": ");
                            System.out.println(value);
                            info[2] = value;
                            break;
                        case "Content-Type":
                            System.out.print(header + ": ");
                            System.out.println(value);
                            info[3] = value;
                            break;
                        case "Content-Length":
                            System.out.print(header + ": ");
                            System.out.println(value);
                            info[4] = value;
                            break;
                        default:
                            continue;
                    }
                }
                
                Vector<InputStream> partStream = new Vector<>();
                
                return res.bodyToFlux(DataBuffer.class)
                    .collect(
                        () -> { return new InputStreamCollector(partStream, 5251073); } ,
                        new BiConsumer<InputStreamCollector, DataBuffer>() {
                            @Override
                            public void accept (InputStreamCollector inputStreamCollector, DataBuffer dataBuffer) {
                                InputStream chunkStream = dataBuffer.asInputStream();
                                long size = partBytes.addAndGet(dataBuffer.readableByteCount());
                                InputStream uploadPartStream;
                                
                                try {
                                /* Progressively encode chunks of ~5MB to
                                 * save to disk or push to remote storage
                                 */
                                    
                                    inputStreamCollector.collectInputStream(chunkStream, size);
                                    
                                    if (partBytes.get() > (mark - 8193)) {
                                        int number = chunks.getAndAdd(1);
                                        totalBytes.addAndGet(size);
                                        System.out.println(
                                            "Encode chunk " + number +" to InputStream: " + size + " bytes");
    
                                        /* Do something with inputStreamCollector.getInputStream() */
                                        // ...
                                        
                                        partBytes.set(0);

                                        if (inputStreamCollector.getInputStream() != null) {
                                            inputStreamCollector.getInputStream().close();
                                            inputStreamCollector.clear();
                                        }
                                    }
                                    
                                } catch (IOException e) {
                                    System.out.println(e.getMessage());
                                }
                            }
                        })
                    .map((InputStreamCollector streamCollector) -> {
                        String md5 = "";
                        int number = chunks.get();
                        long size = partBytes.get();
                        totalBytes.addAndGet(size);
                        
                        try {
                        /* Encode remaining chunks either to final
                         * part for assembly or simply save to local
                         * or remote storage if file size < 5MB
                         */
                            InputStream checkStream = streamCollector.getInputStream();
                            int s = streamCollector.getSize();
                            
                            System.out.println("fluxStream " + checkStream + " has grown to :" + s + " bytes");

                            if (number > 1) {

                                if (partBytes.get() <= (mark - 8193)) {
                                    InputStream uploadPartStream;

                                    System.out.println("Encode final part for assembly of "+ url);

                                    totalBytes.addAndGet(size);
                                    System.out.println(
                                        "Encode chunk " + number + " to InputStream: " + size + " bytes");
                                    
                                    /* Do something with streamCollector.getInputStream() */
                                    // ...

                                    partBytes.set(0);
    
                                    if (streamCollector.getInputStream() != null) {
                                        streamCollector.getInputStream().close();
                                        streamCollector.clear();
                                    }

                                }

                            } else {
                                System.out.println("Save complete file of "+ url);
                                
                                /* Do something with entire streamCollector.getInputStream() */
                                // ...
                            }
                            
                        } catch (IOException e) {
                            System.out.println("Error reading current fluxStream: " + streamCollector.getInputStream());
                            System.out.println(e);
                        }

                        return info;
                    });
            });
            
            return flatMapResult;
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
            retry--;
        }
        
        return Flux.<String[]>just(info);
    }
    
    @NotNull
    public String getBase () {
        return this.base;
    }
}
