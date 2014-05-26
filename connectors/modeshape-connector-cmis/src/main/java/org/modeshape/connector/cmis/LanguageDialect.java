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

        public static Dialect fromString(String fromValue) throws IllegalArgumentException {
            for (Dialect dialect : values()) {
                if (dialect.value.equals(fromValue))
                    return dialect;
            }
            throw new IllegalArgumentException(String.format("Wrong languageDialect parameter '%s' is set",fromValue));
        }

        public String getValue() {
            return value;
        }
    }

    private Dialect dialect;

    public LanguageDialect(String value){
      this.dialect=Dialect.fromString(value);
    }



    public Dialect getDialect(){
        return dialect;
    }
}
