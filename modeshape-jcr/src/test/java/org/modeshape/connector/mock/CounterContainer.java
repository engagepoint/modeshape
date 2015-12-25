/*
 * ModeShape (http://www.modeshape.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
