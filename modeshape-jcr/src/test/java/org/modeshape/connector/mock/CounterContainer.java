package org.modeshape.connector.mock;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by vyacheslav.polulyakh on 10/20/2014.
 */
public final class CounterContainer {

    private AtomicInteger getDocumentByIdCounter = new AtomicInteger(0);

    public int decrementAndGetDocumentByIdCounter() {
        return getDocumentByIdCounter.decrementAndGet();
    }

    public int incrementAndGetDocumentByIdCounter() {
        return getDocumentByIdCounter.incrementAndGet();
    }

    public int getDocumentByIdCounter() {
        return getDocumentByIdCounter.get();
    }

    public void setNewDocumentByIdCounter(int newValue) {
        getDocumentByIdCounter.set(newValue);
    }

    @Override
    public String toString() {
        return "Count : " + getDocumentByIdCounter.get();
    }
}
