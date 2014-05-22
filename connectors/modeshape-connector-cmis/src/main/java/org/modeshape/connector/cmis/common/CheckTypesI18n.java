package org.modeshape.connector.cmis.common;

import org.modeshape.common.i18n.I18n;

/**
 * The internationalized string constants for the <code>org.modeshape.connector.cmis.common.*</code> packages.
 * Created by vyacheslav.polulyakh on 5/14/2014.
 */
public final class CheckTypesI18n {

    public static I18n typeWas;
    public static I18n typeAreChanged;
    public static I18n propertyWas;
    public static I18n propertyAreChanged;
    public static I18n choiceAreChanged;
    public static I18n argumentMayNotBeNull;
    static {
        try {
            I18n.initialize(CheckTypesI18n.class);
        } catch (final Exception err) {
            System.err.println(err);
        }
    }
}
