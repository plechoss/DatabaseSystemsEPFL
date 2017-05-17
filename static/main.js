


$(document).ready(function(){

    // jQuery methods go here...

    $("header ul li").click(function() {
      $(".active").removeClass("active");
      $(this).addClass("active");
    });

    $("#homepageItem").click(function() {
      $("#predefinedQueriesMode").hide();
      $("#searchandinsertMode").hide();
      $("#homepageMode").show();
    });
  
    $("#predefinedQueriesItem").click(function() {
      $("#homepageMode").hide();
      $("#searchandinsertMode").hide();
      $("#predefinedQueriesMode").show();
      
    });
  
    $("#searchandinsertItem").click(function() {
      $("#homepageMode").hide();
      $("#predefinedQueriesMode").hide();
      $("#searchandinsertMode").show();
    });
 
    $("#searchPlus").click(function() {
      $("#advancedSearch").toggle("fast");   
    });
                  
    $("#searchtables").change(function(){
        $(".tables").hide();
        $("#" + $(this).val()).show();
    });
    $("#insertbutton").click(function() {
        $.ajax({
             url: '/insert',
             method: 'POST',
             data: $('#newsearch').serialize(),
             success: function(response) {
             console.log(response);
             },
             error: function(error) {
             console.log(error);
             }
        });
    });
    $("#searchbutton").click(function() {
      $.ajax({
             url: '/search',
             method: 'POST',
             data: $('#newsearch').serialize(),
             success: function(response) {
             console.log(response);
             },
             error: function(error) {
             console.log(error);
             }
             });
      });

});
