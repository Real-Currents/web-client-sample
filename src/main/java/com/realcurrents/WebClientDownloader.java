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
    
    public Flux<String> retrieveFile (String file) throws IOException {
        final AtomicInteger chunks = new AtomicInteger(1);
        final AtomicLong partBytes = new AtomicLong(0);
        final AtomicLong totalBytes = new AtomicLong(0);
        final URI url = URI.create(this.base + file);
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
                        req.headers().forEach(
                            (key, value) -> {
                                System.out.print(key + ": ");
                                System.out.println(value);
                            });
                        return Mono.just(req);
                    })
                )
                .exchange(downloadRequest);
            
            Flux<String> flatMapResult = response.flatMapMany((ClientResponse res) -> {
                System.out.println(res.statusCode());
                
                HttpHeaders httpHeaders = res.headers().asHttpHeaders();
                for (Object header : httpHeaders.keySet().toArray()) {
                    String value = httpHeaders.get(header).toString()
                        .replaceAll("\\[", "")
                        .replaceAll("\\]", "");
                    switch (header.toString()) {
                        case "Content-Disposition":
                            System.out.print(header + ": ");
                            System.out.println(value);
                            break;
                        case "Content-Length":
                            System.out.print(header + ": ");
                            System.out.println(value);
                            break;
                        case "Content-Type":
                            System.out.print(header + ": ");
                            System.out.println(value);
                            break;
                        default:
                            continue;
                    }
                }
                
                Vector<InputStream> partStream = new Vector<>();
                
                return res.bodyToFlux(DataBuffer.class)
                    .collect(
                        InputStreamCollector::new,
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
                                        
                                        partBytes.set(0);

                                        partStream.clear();

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

//                                    String md5;
//                                    uploadPartStream = new SequenceInputStream(partStream.elements());
//                                    md5 = DatatypeConverter.printBase64Binary(Md5Utils.computeMD5Hash(uploadPartStream));

                                    totalBytes.addAndGet(size);
                                    System.out.println(
                                        "Encode chunk " + number + " to InputStream: " + size + " bytes");


                                    partBytes.set(0);

                                    partStream.clear();

                                }

                            } else {
                                System.out.println("Save complete file of "+ url);
                                
                                /* Do something with streamCollector.getInputStream() */
                                // ...
                            }
                            
                        } catch (IOException e) {
                            System.out.println("Error reading current fluxStream: " + streamCollector.getInputStream());
                            System.out.println(e);
                        }

                        return url.toString();
                    });
            });

//            /* Process Mono<String> synchronously */
//            System.out.println("flatMapMany returned "+
//                flatMapResult.block().block() +" @ "+ LocalDateTime.now() +"\n"+
//                "Uploaded to s3: "+ downloadLocation[1].toString());

//            /* Process Mono<String> asynchronously */
//            System.out.println("flatMapMany " +
//                ((flatMapResult.subscribe(System.out::println).isDisposed()) ?
//                    "finished" : "running") + " @ " + LocalDateTime.now());
            
            return flatMapResult;
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
            retry--;
        }
        
        return Flux.just(url.toString());
    }
    
    @NotNull
    public String getBase () {
        return this.base;
    }
}
