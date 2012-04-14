<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="xml"/>

  <xsl:param name="HOME"/>

  <xsl:template match="configuration">
    <configuration>
      <xsl:copy select="@*"/>
      <xsl:apply-templates/>
    </configuration>
  </xsl:template>

  <xsl:template match="property[name='giraph.zkJar']">
    <property><xsl:text>
    </xsl:text>
    <name><xsl:value-of select="name"/></name><xsl:text>
    </xsl:text><value><xsl:call-template name="subst"><xsl:with-param name="string" select="value"/></xsl:call-template></value>
    </property>
  </xsl:template>

  <xsl:template match="property">
    <xsl:copy-of select="."/>
  </xsl:template>

  <xsl:template name="subst">
    <xsl:param name="string"/>
    <xsl:variable name="first" select="substring-before($string, '$HOME')" /> 
    <xsl:variable name="rest" select="substring-after($string, '$HOME')" /> 
    <xsl:variable name="untilcolon"><xsl:value-of select="substring-before($rest,':')"/></xsl:variable>
    <xsl:variable name="end"><xsl:choose><xsl:when test="$untilcolon = ''"><xsl:value-of select="$rest"/></xsl:when><xsl:otherwise><xsl:value-of select="$untilcolon"/></xsl:otherwise></xsl:choose></xsl:variable>
    <xsl:if test="$rest != ''"><xsl:value-of select="$HOME"/><xsl:value-of select="$end"/>:<xsl:call-template name="subst"><xsl:with-param name="string" select="$rest"/></xsl:call-template></xsl:if>
  </xsl:template>

</xsl:stylesheet>
