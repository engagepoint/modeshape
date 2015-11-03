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
package org.modeshape.jcr.security;

import org.modeshape.jcr.ModeShapeRoles;

/**
 * runtime roles
 */
public class Roles {

    private String readonly = ModeShapeRoles.READONLY;
    private String readwrite = ModeShapeRoles.READWRITE;
    private String admin = ModeShapeRoles.ADMIN;

    public Roles() {}

    public Roles(String readonly, String readwrite, String admin) {
        this.readonly = readonly;
        this.readwrite = readwrite;
        this.admin = admin;
    }

    public String getReadOnly() {
        return readonly;
    }

    public String getReadwrite() {
        return readwrite;
    }

    public String getAdmin() {
        return admin;
    }
}
