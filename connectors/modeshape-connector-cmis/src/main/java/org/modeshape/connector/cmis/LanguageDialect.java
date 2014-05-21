package org.modeshape.connector.cmis;



/**
 * @author bogdan.ponomarchuk
 */
public class LanguageDialect {

    public enum Dialect {
        OPENCMIS("opencmis"),
        FILENET("filenet"),
        OTHER("other");
        private String value;

        private Dialect(String value) {
            this.value = value;
        }

        public static Dialect fromValue(String fromValue) {
            for (Dialect dialect : values()) {
                if (dialect.value.equals(fromValue))
                    return dialect;
            }
            return null;
        }

        public String getValue() {
            return value;
        }
    }

    private Dialect dialect;

    protected LanguageDialect(Dialect dialect){
      this.dialect=dialect;
    }

    public static LanguageDialect valueOf(String value) {
       return new LanguageDialect(Dialect.fromValue(value));
    }

    public Dialect getDialect(){
        return dialect;
    }
}
