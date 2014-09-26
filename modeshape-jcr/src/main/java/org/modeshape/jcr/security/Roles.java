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
