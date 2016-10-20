<?xml version="1.0"?>

<!--converting the xml output by modhmms_scampi (with the -L option enabled) to text format -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="text" indent="no"/>
  <xsl:template match="/">
      <xsl:for-each select="hmms/hmm">
      <xsl:text># Scores for HMM: '</xsl:text><xsl:value-of select="hmm_name"/><xsl:text>'
</xsl:text>
    <xsl:for-each select="seqs/seq">
Seq ID: <xsl:value-of select="pure_seq_name_a"/>

      <xsl:text>
Seq length: </xsl:text><xsl:value-of select="getScores/seqlength"/><xsl:text>
</xsl:text>

    <xsl:choose>
      <xsl:when test="getScores/isTmProtein ='yes'">
        <xsl:text>Is TM protein
</xsl:text>
      </xsl:when>
      <xsl:when test="getScores/isTmProtein ='no'">
        <xsl:text>No TM protein
</xsl:text>
      </xsl:when>
</xsl:choose>

<xsl:text>NormalizedLogLikelihood: </xsl:text><xsl:value-of select="getScores/normalizedLogLikelihood"/><xsl:text>
</xsl:text>
<xsl:text>Logodds: </xsl:text><xsl:value-of select="getScores/logodds"/><xsl:text>
</xsl:text>
<xsl:text>Reversi: </xsl:text><xsl:value-of select="getScores/reversi"/><xsl:text>
</xsl:text>

      <xsl:text>Labeling: </xsl:text>

      <xsl:for-each select="getScores/labels/label">
          <xsl:value-of select="."/> 
          <!--<xsl:if test="position() mod 60 = 0">-->
      <!--<xsl:text>-->
<!--</xsl:text>-->
          <!--</xsl:if>-->
      </xsl:for-each> 
      <xsl:text>
</xsl:text>

      <xsl:for-each select="getScores/posteriorprobabilities/labels/label">
          <xsl:value-of select="."/> <xsl:text>     </xsl:text>
      </xsl:for-each>

      <xsl:for-each select="getScores/posteriorprobabilities/post_prob_label_matrix/seq">
          <!--the following two lines' of code add a newline-->
          <xsl:text>
</xsl:text>
          <xsl:for-each select="post-prob">
              <xsl:value-of select="."/> <xsl:text> </xsl:text>
          </xsl:for-each>
      </xsl:for-each>

      <xsl:text>
</xsl:text>

    </xsl:for-each>
    </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>


