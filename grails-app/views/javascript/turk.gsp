<script type="application/javascript">
jQuery(document).ready(function() {

    jQuery("form").attr("action",'${submiturl}');
    if ('${assignmentId}'=='ASSIGNMENT_ID_NOT_AVAILABLE') {
        jQuery(":submit").hide();
    } else {
        jQuery("form").prepend("<input type='hidden' name='assignmentId' value='${assignmentId}'/>");
        jQuery("form").prepend("<input type='hidden' name='workerId' value='${workerId}'/>");
    }
});
</script>
