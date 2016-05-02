package io.blobkeeper.server.handler;

import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.multipart.*;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CustomHttpDataFactory implements HttpDataFactory {
    /**
     * Keep all HttpDatas until cleanAllHttpDatas() is called.
     */
    private final Map<HttpRequest, List<HttpData>> requestFileDeleteMap = PlatformDependent.newConcurrentHashMap();

    public CustomHttpDataFactory() {
    }

    /**
     * @return the associated list of Files for the request
     */
    private List<HttpData> getList(HttpRequest request) {
        List<HttpData> list = requestFileDeleteMap.get(request);
        if (list == null) {
            list = new ArrayList<>();
            requestFileDeleteMap.put(request, list);
        }
        return list;
    }

    @Override
    public Attribute createAttribute(HttpRequest request, String name) {
        return new MemoryAttribute(name);
    }

    @Override
    public Attribute createAttribute(HttpRequest request, String name, String value) {
        try {
            return new MemoryAttribute(name, value);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public FileUpload createFileUpload(HttpRequest request, String name, String filename,
                                       String contentType, String contentTransferEncoding, Charset charset,
                                       long size) {
        FileUpload fileUpload = new DiskFileUpload(name, filename, contentType,
                contentTransferEncoding, charset, size);
        List<HttpData> fileToDelete = getList(request);
        fileToDelete.add(fileUpload);
        return fileUpload;
    }

    @Override
    public void removeHttpDataFromClean(HttpRequest request, InterfaceHttpData data) {
        if (data instanceof HttpData) {
            List<HttpData> fileToDelete = getList(request);
            fileToDelete.remove(data);
        }
    }

    @Override
    public void cleanRequestHttpDatas(HttpRequest request) {
        List<HttpData> fileToDelete = requestFileDeleteMap.remove(request);
        deleteHttpData(fileToDelete);
    }

    @Override
    public void cleanAllHttpDatas() {
        Iterator<Map.Entry<HttpRequest, List<HttpData>>> i = requestFileDeleteMap.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<HttpRequest, List<HttpData>> e = i.next();
            i.remove();

            List<HttpData> fileToDelete = e.getValue();
            deleteHttpData(fileToDelete);
        }
    }

    /**
     * DiskFileUpload elements will be deleted later
     */
    private void deleteHttpData(List<HttpData> fileToDelete) {
        if (fileToDelete != null) {
            fileToDelete.stream().filter(data -> !(data instanceof DiskFileUpload)).forEach(HttpData::delete);
            fileToDelete.clear();
        }
    }
}
